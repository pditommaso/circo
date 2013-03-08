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
import circo.messages.JobFinalize
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
import circo.model.TaskId
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

    /**
     * The address of the current cluster leader node
     */
    Address leaderAddress

    /**
     * The address of this node
     */
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
        tasksDispatcher[JobFinalize] = this.&handleJobFinalize

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
        node = store.getNode(nodeId)
        if( !node ) {
            node = new NodeData()
            node.id = nodeId
            node.master = new WorkerRef(self())
            node.address = cluster.selfAddress()
            node.status = NodeStatus.ALIVE
            node.startTimestamp = System.currentTimeMillis()
            node.storeMemberId = store.localMemberId()
            store.saveNode(node)
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
        store.saveNode(node)

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
            store.saveNode(node)
        }

        else {
            unhandled(message)
        }

    }


    protected TaskId pollNextTaskId() {
        TaskId result
        while( true ) {
            result = node.queue.poll()

            // queue empty -- just return null
            if( !result ) return null

            // check if this task id is marked as killed,
            // when so flag it as kill and pool another item
            if ( store.removeFromKillList(result) ) {
                log.debug "Task ${result} killed -- skipping it"
                def saved = store.updateTask(result) { TaskEntry task -> task.killed = true }
                if ( !saved ) {
                    log.warn "Cannot udpate task: ${result}"
                }
                else {
                    log.debug "Updated task: {$result} -- ${ store.getTask(result).dump() }"
                }
            }

            // it is a valid task id, return it
            else {
                return result
            }

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
        assert message

        message.taskId.each { TaskId taskId ->

            // -- verify than an entry exists in the db
            final task = store.getTask(taskId)
            if( !task ) {
                // TODO improve this error condition handling
                log.warn "Cannot find any task entry with id: '${taskId}' -- message discarded"
                return
            }

            handleWorkToSpool0(task)
        }

    }

    protected void handleWorkToSpool0( TaskEntry task ) {

        // --
        // when the task has already been executed by this node
        // try to forward it to another node
        if( task.worker && node.hasWorkerData(task.worker) && allMasters.size()>1 ) {
            def target = someoneElse()
            log.debug "=> fwd ${WorkToSpool.simpleName} TO someoneElse: ${someoneElse()}"
            target.forward(new WorkToSpool(task.id), getContext())
        }

        // -- add the jobId to the queue - and - notify workers
        else {
            log.debug("Adding task with id: ${task.id} to queue")
            node.queue.add(task.id)

            // update the job status
            task.status = TaskStatus.PENDING
            task.ownerId = nodeId
            store.saveTask(task)

            // mark the task to which it is required to collect a result
            store.addToSink(task)
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

            def list =  store.findNodesByAddressAndStatus( worker.address(), NodeStatus.ALIVE )
            def otherMaster = list?.size()==1 ? list.get(0).master: null

            if ( !otherMaster )  {
                log.warn("Unable to find a valid master to which reply for work request received from: ${worker}")
                return
            }

            if( otherMaster.address() == selfAddress ) {
                log.warn("Oops. Foreign master cannot have the current master address")
                return
            }

            final taskId = pollNextTaskId()
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
         * extract a task from the queue
         */
        final taskId = pollNextTaskId()
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
    protected void handleWorkIsDone( WorkIsDone message ) {

        // -- free the worker
        //    note: worker field can be null when the message is sent during a 'task' recovering operation
        //    see method 'recoverDeadTasks'
        if( message.worker && !node.removeTaskId(message.worker) ) {
            log.error("Blurgh! ${message.worker} said it's done work but we didn't know about him")
        }

        def task = store.getTask( message.taskId )
        if ( task ) {
            handleWorkIsDone0( task )
        }
        else {
            log.error("Blurgh! ${message.worker} said it's done work but cannot find task with id: ${message.taskId}")
        }

        // -- when a shutdown is on-going, kill all the worker actors
        if ( shuttingDown && node.numOfBusyWorkers()==0 ) {
           killWorkers()
        }

    }

    private void handleWorkIsDone0( TaskEntry task ) {
        assert task

        // -- check if the task  i terminated successfully
        //    or if it failed it may be re-executed
        if( task.isRetryRequired() ) {
            log.debug "Task ${task.id} not successfully terminated. Retry to re-execute it -- ${task.dump()}"
            handleWorkToSpool0( task )
            return
        }


        // -- remove the 'sink' flag to signal that the task is terminated (whatever with success or error)
        def removed = store.removeFromSink(task)
        if ( !removed ) {
            log.warn "Oops. Unable to remove sink flag for task id: ${task.id} -- ${task.dump()}"
        }

        // -- notify the client for the available result
        if( removed && task.sender && task.req.notifyResult && !task.aborted ) {
            log.debug "Reply job result to sender -- ${task.id}"
            final reply = new ResultReply( task.req.requestId, task.result )
            task.sender.tell ( reply, self() )
        }

        // -- send the JobComplete message to verify if the Job is terminated
        self().tell ( new JobFinalize(task), null )

    }

    /**
     * The job execution may be composed by several tasks.
     * When a task terminates the message {@code JobFinalize} is sent to verify
     * if the all tasks have been executed or still some are pending.
     * <p>
     * When all tasks are terminated the {@code Job} instance it terminated as well
     *
     * @param message
     */
    protected void handleJobFinalize( JobFinalize message )  {

        final task = message.task

        try {
            boolean done = store.updateJob( task.req.requestId ) { Job job ->
                // update the job status based the passed task
                jobComplete0(job, task)
            }

            if ( done ) {
                Job job = store.getJob(task.req.requestId)
                log.debug "** Job ${job.shortReqId} completed by task: ${task.id} -- ${job.dump()}"
                notifyJobComplete(job)
            }
            else {
                log.debug "Job ${task?.req?.shortReqId} updated not required by task: ${task.id} "
            }

        }
        catch( Exception e ) {
            log.error "Cannot update job request: ${task.req?.shortReqId} while processing task id: ${task.id} -- ${task.dump()}", e
            // TODO Notify error ?
            // TODO stop current execution ?
        }

    }

    private void jobComplete0( Job job, TaskEntry task ) {

        // 'submitted' -> 'success'
        // 'submitted' -> 'failed'

        if ( job.isKilled() ) {
            log.debug "Job '${job.shortReqId}..' KILLED -- nothing to do"
            return
        }
        else if( !job.isRunning() ) {
            log.debug "Oops. Job '${job.shortReqId}..' not in RUNNING status -- ${job.dump()} -- skipping operation"
            return
        }

        final missingTasks = store.countTasksMissing(job.requestId)
        if ( missingTasks ) {
            log.debug "Job '${job.shortReqId}..' missing tasks: ${missingTasks} out ${job.numOfTasks}"
            return
        }

        // -- get all the tasks for this job
        // -- check when all tasks are successfully terminated - or - any of them is failed
        def allTasks = store.findTasksByRequestId(job.requestId)
        boolean onError = allTasks.any { TaskEntry it -> it.isFailed() }

        /*
         * set the final Job 'status' this will be saved on terminate
         */
        job.status = onError ? JobStatus.ERROR : JobStatus.SUCCESS

        // -- create the resulting job context when the job is terminating with success
        if ( job.isSuccess() ) {
            def allResultContext = allTasks.collect { TaskEntry it -> it.result?.context }

            def newContext = job.input ? Context.copy(job.input) : new Context()
            allResultContext.each { Context delta ->
                newContext += delta
            }

            // assign it to the job
            job.output = newContext
        }



    }

    private void notifyJobComplete( Job job )  {

        if ( !job.sender ) {
            // nobody to notify
            return
        }

        if ( job.success ) {
            // -- notify success
            def reply = new JobReply( job.requestId )
            reply.success = true
            reply.context = job.output
            job.sender.tell( reply )
        }
        else if ( job.failed ) {
            // notify failure
            job.sender.tell( new JobReply( job.requestId ) )
            // TODO send task error message to the client ?
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
        store.listNodes().each { NodeData it ->
            if( it.queue.size()>max && it.address != selfAddress && it.status == NodeStatus.ALIVE ) {
                max = it.queue.size()
                node = it
            }
        }

        return node ? allMasters[node.address] : null
    }


    protected void manageMemberDowned( Address nodeAddress ) {
        assert nodeAddress

        List<NodeData> nodes = store.findNodesByAddress(nodeAddress)
        if( !nodes ) {
            log.debug "No NodeData for address: ${nodeAddress} -- ignore it"
            return
        }


        // -- make sure does not exist duplicates for that IP
        NodeData nodeDead
        if ( nodes.size() > 1 ) {
            nodeDead = nodes.sort { NodeData it -> it.id }.find()
            log.warn "*** Multiple for ALIVE node for the same addres: $nodeAddress -- ${nodes.collect{'\n' + it.dump()} } -- used one with id: ${nodeDead.id}"
        }
        else {
            nodeDead = nodes.get(0)
        }


        // now update the node -- this may happens concurrently
        // so the first that is able to set the node status to DEAD
        // is the one that will recover the missing tasks
        boolean done = store.updateNode( nodeDead.id ) { NodeData node ->

            if ( !node.isDead() ) {
                node.status = NodeStatus.DEAD    // set the node to dead
                node.address = null              // <-- clear the address to avoid node addresses overlapping
                node.storeMemberId = null
                node.queue.clear()
            }

        }

        if ( done ) {
            log.debug "Node '${nodeDead.id}' status updated to ${NodeStatus.DEAD}"
            // now recover dead tasks
            recoverDeadTasks(nodeDead.id)
        }
        else {
            log.debug "Node '${nodeDead.id}' status NOT updated to ${NodeStatus.DEAD} -- somebody else done, hopefully .."
        }

    }

    protected void recoverDeadTasks(Integer nodeId) {
        assert nodeId

        List<TaskEntry> tasksToRecover = store.findTasksByOwnerId(nodeId)
        log.debug "Tasks to recover: ${tasksToRecover.size() ?: 'none'}"

        tasksToRecover.each { TaskEntry task -> handleWorkIsDone0(task) }

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


