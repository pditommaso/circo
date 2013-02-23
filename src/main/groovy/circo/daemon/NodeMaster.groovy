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

package circo.daemon
import java.util.concurrent.TimeUnit

import akka.actor.ActorRef
import akka.actor.Address
import akka.actor.Cancellable
import akka.actor.PoisonPill
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
import circo.data.DataStore
import circo.messages.NodeShutdown
import circo.messages.PauseWorker
import circo.messages.ResumeWorker
import circo.messages.WorkIsDone
import circo.messages.WorkToBeDone
import circo.messages.WorkToSpool
import circo.messages.WorkerCreated
import circo.messages.WorkerFailure
import circo.messages.WorkerRequestsWork
import circo.model.Context
import circo.model.Job
import circo.model.JobStatus
import circo.model.NodeData
import circo.model.NodeStatus
import circo.model.TaskEntry
import circo.model.TaskStatus
import circo.model.WorkerData
import circo.model.WorkerRef
import circo.reply.JobReply
import circo.reply.ResultReply
import groovy.transform.ToString
import groovy.util.logging.Slf4j
import scala.concurrent.duration.FiniteDuration
/**
 *  Based on
 *  http://letitcrash.com/post/29044669086/balancing-workload-across-nodes-with-akka-2
 *
 *  @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@Mixin(NodeCategory)
class NodeMaster extends UntypedActor  {

    static final ACTOR_NAME = "Master"

    @Singleton
    @ToString(includePackage = false)
    static class WorkersNotifier { }

    /**
     * The unique node identifier
     */
    final int nodeId

    /**
     * Holds the node runtime data
     */
    protected NodeData node

    /**
     * Connection to the data store
     */
    protected DataStore store

    /**
     * The reference to the current cluster
     */
    Cluster cluster

    /** The address of the current cluster leader node */
    Address leaderAddress

    /** The address of this node */
    Address selfAddress

    private Map<Address,ActorRef> allMasters = new HashMap<>()

    private Random random = new Random()

    private boolean shuttingDown

    private Map<Class,Closure> tasksDispatcher = new HashMap()

    private Map<Class,Closure> clusterDispatcher = new HashMap()

    /**
     * The reference to the scheduler which notifier workers that tasks are available
     */
    private Cancellable notifier

    def createDispatchTable() {

        clusterDispatcher[CurrentClusterState] = this.&handleCurrentClusterState
        clusterDispatcher[LeaderChanged] = this.&handleLeaderChanged

        clusterDispatcher[MemberJoined] = this.&handleMemberCreated
        clusterDispatcher[MemberUp] = this.&handleMemberCreated

        clusterDispatcher[MemberExited] = this.&handleMemberTerminated
        clusterDispatcher[MemberDowned] = this.&handleMemberTerminated
        clusterDispatcher[MemberRemoved] = this.&handleMemberTerminated
        clusterDispatcher[MemberLeft] = this.&handleMemberTerminated


        tasksDispatcher[WorkToSpool] = this.&handleWorkToSpool
        tasksDispatcher[WorkerCreated] = this.&handleWorkerCreated
        tasksDispatcher[WorkerRequestsWork] = this.&handleWorkerRequestWork
        tasksDispatcher[WorkIsDone] = this.&handleWorkIsDone
        tasksDispatcher[WorkerFailure] = this.&handleWorkerFailure
        tasksDispatcher[PauseWorker] = this.&handlePauseWorker
        tasksDispatcher[ResumeWorker] = this.&handleResumeWorker

        tasksDispatcher[Terminated] = this.&handleTerminated
        tasksDispatcher[NodeShutdown] = this.&handleShutdown
    }

    protected Map getAllMasters() { allMasters }


    /**
     * Creates this master actor
     *
     * @param store The connection to the {@code DataStore}
     * @param nodeId The unique identifier of this node
     */
    def NodeMaster( DataStore store, int nodeId = Integer.MAX_VALUE ) {
        assert store
        this.nodeId = nodeId
        this.store = store
        createDispatchTable()
    }

    /**
     * Initialize the Master actor, mainly
     * <li>create the {@code NodeData} instance holding the data structure for this node
     * <li>subscribe for cluster members events
     * <li>schedule a trigger that periodically checks if new pending tasks are available and send them to the worker(s)
     */
    @Override
    def void preStart() {
        setMDCVariables()

        log.debug "++ Starting actor ${self().path()}"
        cluster = Cluster.get(getContext().system())
        selfAddress = cluster.selfAddress()

        // Whenever the node not exist - or - exists but is a status
        // different from 'ALIVE', create a new node structure.
        // This is important to avoid 'dirty' data from a previous 'ALIVE' node
        // that has been stopped, enter in the new node
        node = store.getNodeData(nodeId)
        if( !node ) {
            node = new NodeData()
            node.id = nodeId
            node.master = new WorkerRef(self())
            node.address = cluster.selfAddress()
            node.status = NodeStatus.ALIVE
            node.startTimestamp = System.currentTimeMillis()
            store.storeNodeData(node)
        }
        else {
            log.debug "** Re-using node data: ${node.dump()}"
        }


        // -- listen for member events
        cluster.subscribe(getSelf(), ClusterEvent.LeaderChanged)
        cluster.subscribe(getSelf(), ClusterEvent.ClusterDomainEvent)
        cluster.subscribe(getSelf(), MemberEvent)

        // -- add itself to members map
        allMasters.put(selfAddress, getSelf())

        FiniteDuration duration = new FiniteDuration( 1, TimeUnit.SECONDS )
        getContext().system().scheduler().schedule(duration, duration, getSelf(), WorkersNotifier.instance, getContext().dispatcher())

    }

    /**
     * After the actor stops, does:
     * <li>store the current {@code NodeData} structure
     * <li>if a shutdown request has been submitted, close the connection with the current {@code DataStore}
     */
    @Override
    def void postStop() {
        setMDCVariables()

        log.debug "~~ Stopping actor ${getSelf().path()}"
        store.storeNodeData(node)

        if ( shuttingDown ) {
            log.debug "Shutting down Hazelcast"
            try { store.shutdown() } catch( Exception e ) { log.warn "Error closing hazelcast", e }
        }
    }

    /**
     * Notify worker actors when new tasks are available
     *
     * @param message Not used
     */
    protected void notifyWorkers() {

        if ( node.status == NodeStatus.PAUSED ) {
            return
        }

        // -- notify the workers if required
        def workers = node.freeWorkers()

        // there must be at at least one free worker
        // a task some where i.e. in the local queue or somewhere else
        if ( workers && (node.queue || someoneWithWork()) ) {
            workers.each { WorkerData it ->
                self().tell( new WorkerRequestsWork(it.worker), null )
            }
        }

    }

    /**
     * Main message dispatcher method
     *
     * @param message The message received to be managed
     */
    @Override
    def void onReceive( def message ) {
        setMDCVariables()

        def type = message?.class
        if( clusterDispatcher.containsKey(type) ) {
            log.debug "<< ${type.simpleName} >>"
            clusterDispatcher[type].call( message )
        }

        else if ( message == WorkersNotifier.instance ) {
            notifyWorkers()
        }

        else if ( tasksDispatcher.containsKey(type) ) {
            log.debug "<- $message"
            tasksDispatcher[type].call(message)
            store.storeNodeData(node)
        }

        else {
            unhandled(message)
        }

    }

    /**
     * A worker died. If he was doing anything then we need
     * to give it to someone else so we just add it back to the
     * master and let things progress as usual
     */
    protected void handleTerminated(Terminated message) {

        def worker = new WorkerRef(message.actor)

        def taskId =  node.currentTaskIdFor(worker)
        if( taskId ) {
            log.error("Blurgh! ${worker} died while processing: ${taskId}")
            // Send the work that it was doing back to ourselves for processing
            someoneElse().tell(WorkToSpool.of(taskId), self())
        }

        log.debug "Removing worker: ${worker} from availables workers"
        node.removeWorkerData(worker)

        if ( shuttingDown && node.workers?.size() == 0 ) {
            killMyself()
        }

    }

    /**
     * -- A client has post a new command to be processed
     *    It is appended to the list of jobs
     */

    protected void handleWorkToSpool(WorkToSpool message) {
        final taskId = message.taskId

        // -- verify than an entry exists in the db
        final task = store.getTask(taskId)
        if( !task ) {
            // TODO improve this error condition handling
            log.warn "Cannot find any task entry with id: '${taskId}' -- message discarded"
            return
        }

        // --
        // when the task has already been executed by this node
        // try to forward it to another node
        if( task.worker && node.hasWorkerData(task.worker) && allMasters.size()>1 ) {
            def target = someoneElse()
            log.debug "=> fwd: ${message} TO someoneElse: ${someoneElse()}"
            target.forward(message,getContext())
        }
        // -- add the jobId to the queue - and - notify workers
        else {
            log.debug("Adding task with id: ${taskId} to queue")
            node.queue.add(taskId)

            // update the job status
            task.status = TaskStatus.PENDING
            task.ownerId = nodeId
            store.storeTask(task)

        }
    }


    /**
     * Worker is alive. Add him to the list, watch him for
     * death, and let him know if there's work to be done
     */

    protected void handleWorkerCreated(WorkerCreated message) {

        def worker = message.worker
        context.watch(worker.actor)

        node.putWorkerData( WorkerData.of(worker) )

    }



    /**
     * A worker wants more work.  If we know about him, he's not
     * currently doing anything, and we've got something to do,
     * give it to him.
     */
    protected void handleWorkerRequestWork(WorkerRequestsWork message) {

        final worker = message.worker

        if( node.status == NodeStatus.PAUSED ) {
            log.debug "Node is paused -- ignore request"
            return
        }

        if ( shuttingDown ) {
            log.debug "System is going to shutdown -- ignore the request"
            return
        }

        // de-queue a new jobId to be processed
        // assign to the worker a new job to be processed
        final isLocalWorker = node.hasWorkerData(worker)

        /*
         * if the worker that sent the request does not belong to this node,
         * the Task have to be moved into that node queue
         */
        if( !isLocalWorker ) {
            // avoid that nodes keep continuing stealing tasks each others !!
            if( node.queue.size() < node.numOfFreeWorkers()  ) {
                log.debug "Free workers are available -- ignore request for work from remote node"
                return
            }

            def list =  store.findNodeDataByAddressAndStatus( worker.address(), NodeStatus.ALIVE )
            def otherMaster = list?.size()==1 ? list.get(0).master: null

            if ( !otherMaster )  {
                log.warn("Unable to find a valid master to which reply for work request received from: ${worker}")
                return
            }

            if( otherMaster.address() == selfAddress ) {
                log.warn("Oops. Foreign master cannot have the current master address")
                return
            }

            final taskId = node.queue.poll()
            log.debug "Poll task id: ${taskId?:'-'} -- still in queue: ${node.queue.size()}"

            if( taskId ) {
                log.debug "Send task id: ${taskId} to remote master: $otherMaster"
                otherMaster.tell( WorkToSpool.of(taskId) )
            }

            return
        }

        /*
         * make sure the worker is doing nothing
         */
        if ( node.currentTaskIdFor(worker) ) {
            log.debug "Worker: ${worker.name()} is already busy -- ignore request"
            return
        }

        /*
         * extract a task if from the queue
         */
        final taskId = node.queue.poll()
        log.debug "Poll task id: ${taskId?:'-'} -- still in queue: ${node.queue.size()}"

        if ( !taskId ) {
            def other = someoneWithWork()
            if ( other ) {
                log.debug "No work do be done in this node! => Fwd the request to other node: '$other'"
                other. forward(message, getContext())
            }
        }
        else {
            log.debug "Sending task: ${taskId} to worker: ${worker.name()}"
            node.assignTaskId(worker, taskId)
            worker.tell(new WorkToBeDone(taskId))
        }

    }


    /**
     * When a task completes, remove the current tasks assignment for it
     */
    def void handleWorkIsDone(WorkIsDone message) {

        // -- free the worker
        if( !node.removeTaskId(message.worker) ) {
            log.error("Blurgh! ${message.worker} said it's done work but we didn't know about him")
        }

        def task = store.getTask( message.taskId )
        if ( task ) {
            manageWorkDone( task )
        }
        else {
            log.error("Blurgh! ${message.worker} said it's done work but cannot find task with id: ${message.taskId}")
        }

        // -- when a shutdown is on-going, kill all the worker actors
        if ( shuttingDown && node.numOfBusyWorkers()==0 ) {
           killWorkers()
        }

    }

    protected void manageWorkDone( TaskEntry task ) {
        assert task

        // -- still some work pending, re-schedule to the master
        if( task.isRetryRequired() ) {
            log.debug "Job ${task.id} failed -- retry submitting"
            self().tell( WorkToSpool.of(task.id), null )
            return
        }


        // -- notify the client for the available result
        if( task.sender && task.req.notifyResult ) {
            log.debug "Reply job result to sender -- ${task.id}"
            final reply = new ResultReply( task.req.ticket, task.result )
            task.sender.tell ( reply, self() )
        }

        /*
         * update job action
         */
        try {
            final job = store.getJob( task.req.ticket )

            // -- the status can be changed only from:
            // 'submitted' -> 'success'
            // 'submitted' -> 'failed'
            if( !job.submitted ) {
                return
            }

            // -- if the task is failed, mark the job as failed
            Job updatedJob = Job.copy(job)

            if( !task.isSuccess()  ) {
                updatedJob.status = JobStatus.FAILED
                // TODO stop all pending tasks execution ?
            }
            // -- try to finalize
            else {

                final allTasks = store.findTasksByRequestId( job.requestId )
                final numOfTerm = allTasks?.count { TaskEntry it -> it.terminated }
                if ( numOfTerm < job.numOfTasks ) {
                    log.trace "Completed tasks: ${numOfTerm} of ${job.numOfTasks}"
                    return
                }

                /*
                 * Collect of result context for each tasks
                 */
                def allResultContext = allTasks.collect { TaskEntry it -> it.result?.context }

                def newContext = job.input ? Context.copy(job.input) : new Context()
                allResultContext.each { Context delta ->
                    newContext += delta
                }

                /*
                 * - create the new job output context
                 * - set to success
                 */
                updatedJob.output = newContext
                updatedJob.status = JobStatus.SUCCESS
            }

            def result = store.replaceJob( job, updatedJob )
            if( !result ) {
                log.debug "Cannot update job ${job.dump()} -- with value: ${updatedJob.dump()}"
                return
            }
            log.debug "Job result: ${result.dump()}"

            if ( !updatedJob.sender ) {
                // nobody to notify
                return
            }

            if ( updatedJob.success ) {
                // -- notify success
                def reply = new JobReply( updatedJob.requestId )
                reply.success = true
                reply.context = updatedJob.output
                updatedJob.sender.tell( reply )
            }
            else if ( updatedJob.failed ) {
                // notify failure
                updatedJob.sender.tell( new JobReply( updatedJob.requestId ) )
                // TODO send task error message to the client ?
            }


        }
        catch( Exception e ) {
            log.error "Cannot update job request: ${task.req.ticket} while processing task id: ${task.id}", e
            // TODO Notify error
            // TODO stop current execution ?
        }


    }


    /**
     * A worker send a 'WorkerFailure' message to notify an error condition
     */
    protected void handleWorkerFailure(WorkerFailure message) {

        final worker = message.worker
        node.failureInc(worker)

    }


    /**
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

    /**
     * Resume the node, after it was stopped
     *
     * @param message
     */
    protected void handleResumeWorker(ResumeWorker message) {
        log.debug "Resuming node: $selfAddress"
        node.status = NodeStatus.ALIVE
        notifyWorkers()
    }

    /**
     * Keep track of the current cluster members
     * @param message
     */
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

    /**
     * Keep track of the current cluster leader
     *
     * @param message
     */
    protected void handleLeaderChanged(LeaderChanged message) {
        leaderAddress = message.getLeader()
    }

    /**
     * Add the newly create cluster member to the list of cluster members
     *
     * @param message
     */
    protected void handleMemberCreated(MemberEvent message) {
        addMasterAddress( message.member().address() )
    }

    /**
     * Delete the removed node from the list of cluster members
     * @param message
     */
    protected void handleMemberTerminated(MemberEvent message) {
        removeMasterAddress( message.member().address() )
        manageMemberDowned( message.member().address() )
    }

    /**
     * Add the node {@code Address} to the {@code allMasters} map
     *
     * @param address
     * @return
     */
    protected void addMasterAddress( Address address ) {
        log.debug "Putting address: ${address} in the members map"
        def actor = getContext().system().actorFor("${address}/user/${NodeMaster.ACTOR_NAME}")
        allMasters.put( address, actor)
    }

    /**
     * Remove node {@code Address} from the {@code allMasters} map
     *
     * @param address
     * @return
     */
    protected void removeMasterAddress( Address address ) {
        log.debug "Removing address ${address} from members map"
        allMasters.remove(address)
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
            if( it.queue.size()>max && it.address != selfAddress && it.status == NodeStatus.ALIVE ) {
                max = it.queue.size()
                node = it
            }
        }

        return node ? allMasters[node.address] : null
    }


    protected void manageMemberDowned( Address nodeAddress ) {
        assert nodeAddress

        List<NodeData> nodes = store.findNodeDataByAddress(nodeAddress)
        if( nodes == null ) {
            log.debug "No NodeData for address: ${nodeAddress} -- ignore it"
            return
        }

        if( !nodes ) {
            log.debug "NodeData for address: ${nodeAddress} already managed -- ignore it"
            return
        }

        NodeData nodeFound
        if ( nodes.size() > 1 ) {
            nodeFound = nodes.sort { NodeData it -> it.id }.find()
            log.warn "*** Multiple for ALIVE node for the same addres: $nodeAddress -- ${nodes.collect{'\n' + it.dump()} } -- used one with id: ${nodeFound.id}"
        }
        else {
            nodeFound = nodes.get(0)
        }

        def updatedNode = new NodeData(nodeFound)
        updatedNode.status = NodeStatus.DEAD    // set the node to dead
        updatedNode.address = null              // <-- clear the address to avoid node addresses overlapping

        int count = 0
        // try to update using concurrent replace
        while( !store.replaceNodeData(nodeFound, updatedNode) ) {

            // when fails to update the 'current' node, read it again
            // if the read value is 'DEAD' --> some other node update it to the required status, so skip the operation
            nodeFound = store.getNodeData(nodeFound.id)
            if ( nodeFound.status == NodeStatus.DEAD ) {
                log.debug "Unable to replace node status to ${NodeStatus.DEAD} for address: ${nodeAddress} -- somebody else done it"
                return
            }
            // re-try to set the node to 'DEAD' status
            else {
                updatedNode = new NodeData(nodeFound)
                updatedNode.status = NodeStatus.DEAD
            }

            if( count>10 ) {
                // something wrong
                throw new IllegalStateException("Unable to set node with id: ${nodeFound.id} to status ${NodeStatus.DEAD}" )
            }
        }

        // now record dead
        recoverDeadTasks(updatedNode.id)

    }

    protected void recoverDeadTasks(Integer nodeId) {
        assert nodeId

        List<TaskEntry> tasksToRecover = store.findTasksOwnedBy(nodeId)
        log.debug "Tasks to recover: ${tasksToRecover.size() ?: 'none'}"

        tasksToRecover.each { TaskEntry entry -> manageWorkDone( entry ) }


    }

    /**
     * Start the shutdown process, when the message {@code NodeShutdown} is sent
     *
     * @param message
     */
    protected void handleShutdown(NodeShutdown message) {

        // set the node in 'PAUSED' status
        node.status = NodeStatus.PAUSED
        this.shuttingDown = true
        this.notifier?.cancel()

        if( node.numOfBusyWorkers() > 0 ) {
            // some tasks are running, kill all of theme
            // on completion the workers will be killed
            killTasks()
        }
        else {
            // no tasks running, so kill all the workers
            killWorkers()
        }


    }

    /**
     * Kill all running tasks
     */
    private void killTasks() {

        def kill = new PauseWorker(hard: true) // use hard to kill all on-going tasks
        node.busyWorkers().each { WorkerData it -> it.worker.tell(kill) }
    }

    /**
     * Kill all worker actors
     */
    private void killWorkers() {
        node.workers.keySet().each { WorkerRef ref -> ref.tell(PoisonPill.instance)  }
    }

    /**
     * Shutdown the Akka system
     */
    private void killMyself() {
        log.info "****** finalizing system shutdown ******"
        context().system().shutdown()

    }




}


