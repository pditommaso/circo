/*
 * Copyright (c) 2012, the authors.
 *
 *    This file is part of Circo.
 *
 *    Circo is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation, either version 3 of the License, or
 *    (at your option) any later version.
 *
 *    Circo is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with Circo.  If not, see <http://www.gnu.org/licenses/>.
 */

package circo
import akka.actor.ActorRef
import akka.actor.Address
import akka.actor.Terminated
import akka.actor.UntypedActor
import akka.cluster.Cluster
import akka.cluster.ClusterEvent
import akka.cluster.ClusterEvent.CurrentClusterState
import akka.cluster.ClusterEvent.LeaderChanged
import akka.cluster.ClusterEvent.MemberDowned
import akka.cluster.ClusterEvent.MemberEvent
import akka.cluster.ClusterEvent.MemberExited
import akka.cluster.ClusterEvent.MemberJoined
import akka.cluster.ClusterEvent.MemberLeft
import akka.cluster.ClusterEvent.MemberRemoved
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.Member
import circo.data.*
import circo.messages.*
import circo.reply.ResultReply
import groovy.util.logging.Slf4j
/**
 *  Based on
 *  http://letitcrash.com/post/29044669086/balancing-workload-across-nodes-with-akka-2
 *
 *  @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
class JobMaster extends UntypedActor  {

    static final ACTOR_NAME = "Master"

    // Holds known workers and what they may be working on
    protected NodeData node

    protected DataStore store

    Cluster cluster

    Address leaderAddress

    Address selfAddress

    Random random = new Random()

    Map<Address,ActorRef> allMasters = new HashMap<>()

    int workerCount = 0

    private Map<Class,Closure> dispatchTable = new HashMap()

    private Map<Class,Closure> membersTable = new HashMap()

    def createDispatchTable() {

        membersTable[CurrentClusterState] = this.&handleCurrentClusterState
        membersTable[LeaderChanged] = this.&handleLeaderChanged

        membersTable[MemberJoined] = this.&handleMemberCreated
        membersTable[MemberUp] = this.&handleMemberCreated

        membersTable[MemberExited] = this.&handleMemberTerminated
        membersTable[MemberDowned] = this.&handleMemberTerminated
        membersTable[MemberRemoved] = this.&handleMemberTerminated
        membersTable[MemberLeft] = this.&handleMemberTerminated


        dispatchTable[WorkToSpool] = this.&handleWorkToSpool
        dispatchTable[WorkerCreated] = this.&handleWorkerCreated
        dispatchTable[WorkerRequestsWork] = this.&handleWorkerRequestWork
        dispatchTable[WorkIsDone] = this.&handleWorkIsDone
        dispatchTable[WorkerFailure] = this.&handleWorkerFailure
        dispatchTable[PauseWorker] = this.&handlePauseWorker
        dispatchTable[ResumeWorker] = this.&handleResumeWorker

        dispatchTable[Terminated] = this.&handleTerminated
    }




    /*
     * When a new entry is added to the jobs storage map, the listener is invoked which will add
     * the job id to the jobs local queue
     */
    private onNewJobAddedListener = { JobEntry job ->
        log.debug "Add new Job event received for: $job"
        if( job.status != JobStatus.NEW ) {
            log.debug "Skipping add for job: '${job}' since it is not NEW"
            return
        }

        job.assigned = selfAddress
        store.saveJob(job)

        // add this job to the queue
        def message = WorkToSpool.of(job.id)
        log.debug "-> $message"
        getSelf().tell(message, getSelf())
    }


    def JobMaster( DataStore store ) {
        assert store
        this.store = store
        createDispatchTable()
    }


    @Override
    def void preStart() {
        log.debug "++ Starting actor ${getSelf().path()}"
        cluster = Cluster.get(getContext().system())
        selfAddress = cluster.selfAddress()

        // -- load the data structure for this node - or - create a new one
        node = store.getNodeData(selfAddress)
        if( !node ) {
            node = new NodeData()
            //node.name = InetAddress.getLocalHost()?.getHostName()
            node.status = NodeStatus.AVAIL
            node.address = cluster.selfAddress()
            node.startTimestamp = getContext().system().startTime()
            store.putNodeData(node)
        }

        // -- listen for member events
        cluster.subscribe(getSelf(), ClusterEvent.LeaderChanged)
        cluster.subscribe(getSelf(), ClusterEvent.ClusterDomainEvent)
        cluster.subscribe(getSelf(), MemberEvent)

        // -- add itself to members map
        allMasters.put(selfAddress, getSelf())

        // --- add the event listener
        store.addNewJobListener( onNewJobAddedListener )

    }

    @Override
    def void postStop() {
        log.debug "~~ Stopping actor ${getSelf().path()}"
        store.putNodeData(node)

        // --- remove the event listener
        store.removeNewJobListener( onNewJobAddedListener )
    }


    def void updateNodeInfo(Closure closure) {
        node = store.updateNodeData(node,closure)
    }


    def void notifyWorkers () {

        if( node.queue.isEmpty() ) {
            log.debug "Empty queue -- nothing to notify"
            return
        }

        if ( node.status == NodeStatus.PAUSED ) {
            log.debug "Node pause -- worker wont be notified"
            return
        }

        // -- notify the workers if required
        node.eachWorker { WorkerData entry ->

            if( !entry.currentJobId ) {
                log.debug "-> WorkIsReady() to ${entry.worker}"
                entry.worker.tell( WorkIsReady.getInstance() )
            }
        }
    }


    @Override
    def void onReceive( def message ) {

        def type = message?.class
        if( membersTable.containsKey(type) ) {
            log.debug "<< ${type.simpleName} >>"
            membersTable[type].call( message )
        }

        else if ( dispatchTable.containsKey(type) ) {
            log.debug "<- $message"
            dispatchTable[type].call(message)
            store.putNodeData(node)
        }

        else {
            unhandled(message)
        }

    }

    /*
     * A worker died. If he was doing anything then we need
     * to give it to someone else so we just add it back to the
     * master and let things progress as usual
     */
    protected void handleTerminated(Terminated message) {

        def worker = new WorkerRef(message.actor)

        def jobId =  node.getWorkerData(worker) ?. currentJobId
        if( jobId ) {
            log.error("Blurgh! ${worker} died while processing: ${jobId}")
            // Send the work that it was doing back to ourselves for processing
            someoneElse().tell(WorkToSpool.of(jobId), self())
        }

        log.debug "Removing worker: ${worker} from availables workers"
        node.removeWorkerData(worker)

    }

    /*
     * -- A client has post a new command to be processed
     *    It is appended to the list of jobs
     */

    protected void handleWorkToSpool(WorkToSpool message) {
        final jobId = message.JobId

        // -- verify than an entry exists in the db
        final entry = store.getJob(jobId)
        if( !entry ) {
            // TODO improve this error condition handling
            log.warn "Cannot find any entry for id ${jobId} -- message discarded"
            return
        }

        // -- forward to somebody else
        if( entry.worker?.address() == selfAddress && allMasters.size()>1 ) {
            def target = someoneElse()
            log.debug "=> fwd: ${message} TO someoneElse: ${someoneElse()}"
            target.forward(message,getContext())
        }
        // -- add the jobId to the queue - and - notify workers
        else {
            log.debug("Adding ${jobId} to queue")
            node.queue.add(jobId)

            // update the job status
            entry.status = JobStatus.PENDING
            entry.assigned = selfAddress
            store.saveJob(entry)
            // notify workers
            notifyWorkers()
        }
    }


    /*
     * Worker is alive. Add him to the list, watch him for
     * death, and let him know if there's work to be done
     */

    def void handleWorkerCreated(WorkerCreated message) {

        def worker = message.worker
        context.watch(worker.actor)

        node.putWorkerData( WorkerData.of(worker) )

        // trigger a WorkerRequestsWork event to force a job poll
        // Note: only for the very first worker in the node
        if ( workerCount++ == 0 ) {
            getSelf().tell( new WorkerRequestsWork(worker), getSelf() )
        }
    }



    /*
     * A worker wants more work.  If we know about him, he's not
     * currently doing anything, and we've got something to do,
     * give it to him.
     */
    def void handleWorkerRequestWork(WorkerRequestsWork message) {

        final worker = message.worker

        if( node.status == NodeStatus.PAUSED ) {
            log.debug "Node paused -- Worker request for work ignored"
            return
        }

        // de-queue a new jobId to be processed
        // assign to the worker a new job to be processed
        final isLocalWorker = worker.isLocal(selfAddress)
        final jobId = node.queue.poll()
        if ( !jobId ){
            log.debug "No work available"

            // Worker is local?
            // YES -> ask work to other node
            // NO -> no work to done

            def other
            if( isLocalWorker && (other = someoneWithWork()) ) {
                log.debug "=> Fwd Work Request to: '$other'"
                other. forward(message, getContext())
            }
            return
        }

        // Worker is local ?
        // YES -> update the worker map and and notify the worker
        // NO -> send the work to the other node
        if( !isLocalWorker ) {
            ActorRef otherMaster = allMasters.get( worker.address() )
            log.debug "Send job id: '${jobId}' to remote node [step 1]: '$otherMaster'"
            if( !otherMaster ) {
                otherMaster = someoneElse()
                log.debug "Send id: '${jobId}' remote node [step 2]: '$otherMaster'"
            }
            otherMaster.tell( WorkToSpool.of(jobId), self() )
            return
        }


        // TODO ++++ improve the error condition handling
        try {
            if( !node.hasWorkerData(worker) ) {
                throw new IllegalStateException("Unknown worker: ${worker} for node ${selfAddress}")
            }

            if( !node.assignJobId(worker, jobId) ) {
                throw new IllegalStateException("Oops! Worker ${worker} request a new job but is still processing a job")
            }

            // so far everything is ok -- notify the worker
            def request = new WorkToBeDone(jobId)
            log.debug "-> ${request}"
            worker.tell( request )
        }
        catch( Exception failure ) {

            if( failure instanceof IllegalStateException ) {
                log.warn( failure.getMessage() ?: failure.toString() )
            }
            else {
                log.error("Unable to process message: $message", failure)
            }

            // re-queue the jobId that was unable to manage
            someoneElse().tell( WorkToSpool.of(jobId), self() )
        }

    }


    def void handleWorkIsDone(WorkIsDone message) {

        // free the worker
        if( !node.removeJobId(message.worker) ) {
            log.error("Blurgh! ${message.worker} said it's done work but we didn't know about him")
        }

        // -- request more work
        log.debug "-> WorkerRequestsWork(${message.worker}) to master"
        self().tell( new WorkerRequestsWork(message.worker), self() )

    }



    /*
     * A worker send a 'WorkerFailure' message to notify an error condition
     */
    protected void handleWorkerFailure(WorkerFailure message) {

        final worker = message.worker
        node.failureInc(worker)

    }


    /*
     * pause the node and stop all workers
     */

    protected void handlePauseWorker(PauseWorker message) {

        log.debug "Pausing node: $selfAddress"

        // set the node in 'PAUSED' status
        node.status = NodeStatus.PAUSED

        // -- stop as well all running jobs
        if ( message.hard ) {
            node.workers.keySet().each { WorkerRef ref ->  ref.tell(message) }
        }

    }

    protected void handleResumeWorker(ResumeWorker message) {
        log.debug "Resuming node: $selfAddress"
        node.status = NodeStatus.AVAIL
        notifyWorkers()
    }


    protected void handleCurrentClusterState(CurrentClusterState message) {
        // iterate over all members and add the missing ones
        def currentMembers = message.members.collect { Member it -> it.address() }
        currentMembers.each { it ->
            if( !allMasters.containsKey(it) ) {
                addMasterAddress(it)
            }
        }

        // remove all addresses registered as master BUT not included in the current state
        allMasters.keySet().each { Address it ->
            if( !currentMembers.contains(it) && it != selfAddress ) {
                removeMasterAddress(it)
            }
        }
    }

    protected void handleLeaderChanged(LeaderChanged message) {
        leaderAddress = message.getLeader()
        log.debug "leaderAddres: ${leaderAddress} "
    }

    protected void handleMemberCreated(def message) {
        addMasterAddress( message.member().address() )
    }

    protected void handleMemberTerminated(def message) {
        removeMasterAddress( message.member().address() )
        resumeJobs( message.member().address() )
    }

    protected boolean addMasterAddress( Address address ) {
        log.debug "Putting address: ${address} in the members map"
        def actor = getContext().system().actorFor("${address}/user/${JobMaster.ACTOR_NAME}")
        allMasters.put( address, actor)
        return true
    }

    protected boolean removeMasterAddress( Address address ) {
        log.debug "Removing address ${address} from members map"
        allMasters.remove(address)
        return true
    }

    /**
     *
     * @return
     */
    protected ActorRef getAny() {
        assert allMasters, 'Members map must contain at itself as member'

        // make a copy - and - remove the current address
        List<Address> addresses = new ArrayList(allMasters.keySet())

        // find out a random actor in this list
        def randomAddress = addresses.get(random.nextInt(addresses.size()))
        allMasters.get( randomAddress )

    }

    /**
     *
     * @return
     */
    protected ActorRef someoneElse( ) {
        assert allMasters, 'Members map must contain at itself as member'

        // make a copy - and - remove the current address
        List<Address> addresses = new ArrayList(allMasters.keySet())
        // in order to give priority to other nods
        // if there are at least two nodes, remove the its own address
        if( allMasters.size()>1 ) {
            addresses.remove(selfAddress)
        }

        // find out a random actor in this list
        def randomAddress = addresses.get(random.nextInt(addresses.size()))
        return allMasters.get( randomAddress )
    }

    /**
     *
     * @return
     */
    protected ActorRef someoneWithWork() {
        assert allMasters, 'Members map must contain at itself as member'

        ActorRef actor
        NodeData node = null
        long max = 0
        store.findAllNodesData().each { NodeData it ->
            if( it.queue.size()>max && it.address != selfAddress && it.status == NodeStatus.AVAIL ) {
                max = it.queue.size()
                node = it
            }
        }

        return node ? allMasters[node.address] : null
    }


    protected void resumeJobs( Address nodeAddress ) {
        assert nodeAddress

        final deadNodeData = store.getNodeData(nodeAddress)
        if( deadNodeData == null ) {
            log.debug "No NodeData for address: ${nodeAddress} -- ignore it"
            return
        }

        def OK = store.removeNodeData(deadNodeData)
        if( OK ) {
            log.info "Cleared NodeData for address: ${nodeAddress}"
        }
        else {
            log.warn "Cannot clear NodeData for address: ${nodeAddress} -- luckily someone else has been quickier"
            return
        }


        List<JobEntry> jobsToRecover = store.findAll() .findAll { JobEntry entry -> entry.assigned == nodeAddress }
        log.debug "Jobs to recover: ${jobsToRecover.size() ?: 'none'}"

        jobsToRecover.each { JobEntry entry ->
            log.debug "Recovering job: ${entry}"

            // -- notify the sender the result
            boolean required = entry.retryIsRequired()
            log.debug "Is Retry required for job id: ${entry.id}?: $required"
            if( required ) {
                def target = getAny()
                def message = WorkToSpool.of(entry.id)
                log.debug "Recover pending job id: '${entry.id}' -- sending $message to: ${target}"
                target.tell( message, self() )
            }
            else if( entry.sender ) {
                log.debug "=> Notify sender of result of job: ${entry.id}"
                entry.sender.tell ( new ResultReply( entry.req.ticket, entry.result ) )
            }

        }


    }




}


