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
import akka.actor.ActorRef
import akka.actor.Address
import akka.actor.UntypedActor
import circo.client.AbstractCommand
import circo.client.CmdGet
import circo.client.CmdKill
import circo.client.CmdList
import circo.client.CmdNode
import circo.client.CmdSub
import circo.data.DataStore
import circo.messages.PauseWorker
import circo.messages.ProcessKill
import circo.messages.ResumeWorker
import circo.messages.WorkToSpool
import circo.model.Context
import circo.model.DataRef
import circo.model.Job
import circo.model.JobStatus
import circo.model.NodeData
import circo.model.TaskEntry
import circo.model.TaskId
import circo.model.TaskReq
import circo.model.TaskStatus
import circo.model.WorkerRef
import circo.reply.AckReply
import circo.reply.ListReply
import circo.reply.NodeReply
import circo.reply.ResultReply
import circo.reply.SubReply
import circo.util.CircoHelper
import com.google.common.collect.LinkedListMultimap
import com.google.common.collect.Multimap
import groovy.util.logging.Slf4j
/**
 *
 *  @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */


@Slf4j
@Mixin(NodeCategory)
class FrontEnd extends UntypedActor {

    static final ACTOR_NAME = 'FrontEnd'

    def ActorRef master

    def DataStore dataStore

    private int nodeId

    def Map<Class<? extends AbstractCommand>, Closure> dispatchTable = new HashMap<>()

    /**
     * The front-end actor constructor
     * @param store The {@code DataStore} instance
     * @param nodeId The unique 'node' identifier
     */
    def FrontEnd( DataStore store, int nodeId = 0 ) {
        this.dataStore = store
        this.nodeId = nodeId
    }

    /**
     * Initialize the actor creating the {@code NodeMaster} actor
     */
    def void preStart() {
        setMDCVariables()
        log.debug "++ Starting actor ${getSelf().path()}"

        // if not injected by someone else, get a reference to the 'master' actor -- useful for testing
        if( !master ) {
            master = getContext().system().actorFor("/user/${NodeMaster.ACTOR_NAME}")
        }
    }


    def void postStop() {
        setMDCVariables()
        log.debug "~~ Stopping actor ${getSelf().path()}"
    }


    /*
     * defined the dispatch table that associate the handler method for each command
     */
    {
        dispatchTable[ CmdSub ] = this.&handleCmdSub
        dispatchTable[ CmdNode ] = this.&handleCmdNode
        dispatchTable[ CmdList ] = this.&handleCmdList
        dispatchTable[ CmdKill ] = this.&handleCmdKill

    }

    /**
     * Invokes the associated method handler for each command
     *
     * @param message
     * @return
     */
    protected boolean dispatch( def message ) {
        assert message

        def handler = dispatchTable[ message.class ]
        if( handler ) {
            handler.call(message)
            return true
        }

        return false
    }

    /**
     * Handles the message received by the FrontEnd actor
     *
     * @param message
     */

    @Override
    void onReceive(Object message) {
        setMDCVariables()
        log.debug "<- ${message}"

        if ( !dispatch(message) ){
            unhandled(message)
        }
    }

    /**
     * Handle the {@code CmdGet}
     *
     * @param command The {@code CmdGet} instance
     */
    @Deprecated
    void handleCmdGet( CmdGet command ) {
        assert command

        command.listOfIds?.each { String value ->

            def reply = new ResultReply(command.ticket)
            try {
                def entry = dataStore.getTask(TaskId.of(value))
                if( entry ) {
                    reply.result = entry.result
                }
                else {
                    reply.warn << "Unable to retrieve result for id: '$value'"
                }
            }
            catch( Exception e ) {
                log.error "Error retrieving result for id: '$value'", e
                reply.error << "Error retrieving result for id: '$value'"
            }

            // reply back
            sender().tell(reply, self())
        }
    }

    /**
     * Implements the client 'list' command
     *
     * @param command An instance of {@code CmdList} containing the command as entered by the user
     */
    void handleCmdList( CmdList command ) {
        assert command


        /*
         * create the reply object
         */
        def reply
        if ( command.tasks  ) {
            reply = processListTasks(command)
        }

        else {
            reply = processListJobs(command)
        }

        sender().tell(reply, self())

    }

    ListReply processListJobs(CmdList command) {

        def reply = new ListReply(command.ticket)
        def list = new LinkedList<Job>()

        // filter all jobs by the provided IDs
        if( command.jobsId ) {
            command.jobsId.each { String it ->
                list.addAll( dataStore.findJobsByRequestId(it) )
            }
        }
        // filter ALL jobs
        else if ( command.all ) {
            list.addAll( dataStore.listJobs() )
        }
        // filter the jobs by the specified 'status'
        else {

            List<JobStatus> status = []
            if( command.status ) {
                command.status.each {
                    try {
                        status << JobStatus.fromString(it)
                    }
                    catch( Exception e ) {
                        reply.warn << e.toString()
                    }
                }
            }
            else {
                status = [JobStatus.RUNNING]
            }


            list.addAll(  dataStore.findJobsByStatus( status as JobStatus[] ) )
        }



        /*
         *  Look for all the jobs
         */

        reply.jobs = new LinkedList<ListReply.JobInfo>()
        list.each {
            def info = new ListReply.JobInfo(it)
            reply.jobs << info

            def allTasks = dataStore.findTasksByRequestId(it.requestId)
            // set the command

            // TODO ++ this must change - the TaskReq have to become JobReq
            info.command = allTasks.find()?.req?.script

            allTasks.each{ TaskEntry task ->

                if ( !task.isTerminated() ) {
                    info.numOfPendingTasks++
                }

                // tot number of failed tasks
                if( task.isFailed() ) {
                    info.numOfFailedTasks += 1
                }
            }

        }


        return reply
    }

    ListReply processListTasks(CmdList command) {
        def reply = new ListReply(command.ticket)
        def list = new LinkedList<TaskEntry>()

        if ( command.jobsId ) {
            command.jobsId.each { String reqId ->
                list.addAll( dataStore.findTasksByRequestId( reqId ) )
            }
        }

        // filter by task status
        reply.tasks = new LinkedList<TaskEntry>()
        if( command.status ) {

            // decode the request status from a strings list to an array of TaskStatus
            command.status.each { String str ->
                try {
                    reply.tasks.addAll(dataStore.findTasksByStatusString( str ))
                }
                catch ( Exception e ) {
                    reply.warn << e.getMessage()
                }
            }

        }
        else {

            if ( list ) {
                reply.tasks = list
            }
            else if ( command.all ){
                reply.tasks = dataStore.listTasks()
            }
            else {
                reply.warn << 'provide at least a filtering criteria'
            }

        }

        return reply
    }



    /*
     * The client send a 'TaskReq' to the coordinator in order to submit a new job request
     */
    void handleCmdSub(CmdSub command) {
        assert command

        /*
         * The main Job item
         */
        final job = new Job(command.ticket)
        final reply = new SubReply(command.ticket)

        /*
         * create a new task request for each times required
         */
        List<TaskEntry> listOfTasks = []
        if( command.times ) {
            log.debug "sub times: ${command.times}"
            command.times.withStep().each  { index ->
                listOfTasks << createTaskEntry( command, index )
            }
        }

        /*
         * Create a job for each entry in the 'eachList'
         */
        else if ( command.eachItems ) {
            def missing = []
            command.eachItems.each {
                if( !command.context.contains(it) ) missing << it
            }

            if ( missing ) {
                reply.error << "Unknown context entrie(s): ${missing.join(',')}"
            }
            else {
                def index=0
                command.context.combinations( command.eachItems ) { List<DataRef> variables ->
                    log.debug "Variables combination: ${variables}"
                    def entry = createTaskEntry(command, index++, variables)
                    listOfTasks << entry
                }
            }

        }

        else {
            listOfTasks << createTaskEntry( command )
        }


        /*
         * reply to the sender with TaskId assigned to the received JobRequest
         */
        if ( getSender()?.path()?.name() != 'deadLetters' ) {
            reply.taskIds = listOfTasks .collect { it.id }
            reply.success = listOfTasks.size()>0
            log.debug "Send confirmation to client: $reply"
            getSender().tell( reply, getSelf() )
        }

        if( listOfTasks.isEmpty() ) {
            return
        }


        /*
         * When the jobs are save, the data-store will trigger one event for each of them
         * that will raise the computation
         * Note: the save MUST be after the client notification, otherwise may happen
         * that some jobs terminate (and the termination notification is sent) before the SubReply
         * is sent to teh client, which will cause a 'dead-lock'
         */
        job.status = JobStatus.PENDING
        job.input = command.context ? Context.copy(command.context) : new Context()
        job.sender = new WorkerRef(getSender())
        job.numOfTasks = listOfTasks.size()

        dataStore.saveJob(job)


        /*
         * partition the tasks by sending a spool message to nodes in the cluster
         */
        Multimap<NodeData, TaskId> partitionTasks = LinkedListMultimap.newInstance()
        dataStore.partitionNodes( listOfTasks )  { TaskEntry task, NodeData partitionNode ->

            if ( !partitionNode ) {
                log.warn "Cannot find a partition for task: $task -- using current node"
                partitionNode = dataStore.getNode(nodeId)
            }

            // -- assign the owner node - and - save the new task
            task.ownerId = partitionNode.id
            dataStore.saveTask( task )

            // -- put it task id in the partition list
            partitionTasks.put(partitionNode, task.id)
        }

        // finally notify the nodes
        partitionTasks.keySet().each { NodeData node ->
            log.debug "Queueing tasks to node: ${node.id} <-- ${partitionTasks.get(node)}"
            node.master.tell(  new WorkToSpool( partitionTasks.get(node) )  )
        }

    }

    /**
     * Define the 'kill' command behavior
     *
     * @param command The command as entered by user on client-side
     */
    protected void handleCmdKill( CmdKill command ) {
        assert command

        def listOfJobs = new LinkedHashSet<Job>()
        def listOfTasks = new LinkedHashSet<TaskEntry>()

        def reply = populateKillList(command, listOfJobs, listOfTasks )

        /*
         * Stop on any message
         */
        if( reply.hasMessages() ) {
            sender.tell(reply, self())
            return
        }


        /*
         * Kill HARD Jobs & Tasks
         */
        if ( listOfJobs && listOfTasks ) {

            /*
             * OK proceed with the kill operation
             */
            dataStore.withTransaction {

                listOfJobs.each { Job job ->
                    def done=dataStore.updateJob(job.requestId) { Job thisJob -> thisJob.status = JobStatus.KILLED }
                    log.debug "Killing job ${job.shortReqId} -- done: ${done}"
                }

                listOfTasks.each { TaskEntry task ->
                    //flag the tasks to be killed
                    dataStore.addToKillList( task.id )
                    // the ones still running are killed immediately
                    if ( !task.isTerminated() && task.worker ) {
                        task.worker.tell( new ProcessKill(taskId: task.id) )
                    }
                }

            }
        }

        /*
         * Kill SOFT only the tasks are cancelled, they may be re-submitted
         */
        else if ( listOfTasks ){

            listOfTasks.each { TaskEntry task ->

                if ( !task.isTerminated() && task.worker ) {
                    task.worker.tell( new ProcessKill(taskId: task.id, cancel: true) )
                }

            }
        }
        else {
            log.debug "Nothing to kill -- ${command.dump()}"
        }


        sender.tell(reply, self())
    }


    protected AckReply populateKillList( CmdKill command, Set<Job> listOfJobs, Set<TaskEntry> listOfTasks ) {
        assert command

        def reply = new AckReply(command.ticket)

        // -- give the list of ids
        if( command.itemsToKill ) {

            /*
             * When it is a list of TaskIds, kill only the specified task
             * - the task may be re-executed if attempts remains
             * - the job will not be killed
             */
            if( command.tasks ) {
                command.itemsToKill.each { str ->
                    try {
                        def task = dataStore.getTask(TaskId.of(str))
                        if ( !task ) {
                            reply.info << "No such task: ${str}"
                        }
                        else if ( !task.isRunning() ) {
                            reply.warn "Can't kill task: ${str} -- only running task can be killed"
                        }
                        else {
                            listOfTasks << task
                        }
                    }
                    catch( Exception e ) {
                        reply.warn << "Cannot kill task: ${str} -- ${e.getMessage()}"
                        log.debug "Unable to load task: ${str}", e
                    }
                }
            }

            /*
             * Load as Job IDs
             */
            else {

                command.itemsToKill.each { String str ->
                    def jobsFound = dataStore.findJobsByRequestId( str )
                    if( jobsFound.size() == 0 ) {
                        reply.warn << "No such job '${str}'"
                    }
                    else {
                        listOfJobs.addAll( jobsFound )
                    }

                    // look for the associated tasks
                    jobsFound.each { Job job ->
                        listOfTasks.addAll( dataStore.findTasksByRequestId(job.requestId) )
                    }
                }

            }
        }

        /*
         * Kill ALL pending and running jobs
         */
        else if ( command.all ) {
            // load all pending and running jobs
            listOfJobs.addAll( dataStore.findJobsByStatus( JobStatus.PENDING, JobStatus.RUNNING )  )

            // for each job find the associated tasks
            listOfJobs.each { Job job ->
                listOfTasks.addAll(  dataStore.findTasksByRequestId(job.requestId) )
            }
        }

        /*
         * Something wrong
         */
        else {
            reply.info << 'Nothing to kill'
        }

        return reply
    }

    /**
     * @return Convert this job submit command to a valid {@code TaskReq} instance
     */
    private TaskReq createReq(CmdSub sub) {
        assert sub.ticket
        assert sub.command

        def request = new TaskReq()
        request.requestId = sub.ticket
        request.environment = new HashMap<>(sub.env)
        request.script = sub.command.join(' ')
        request.maxAttempts = sub.maxAttempts
        if ( sub.maxDuration ) {
            request.maxDuration = sub.maxDuration.toMillis()
        }
        if ( sub.maxInactive ) {
            request.maxInactive = sub.maxInactive.toMillis()
        }

        request.user = sub.user
        request.context = sub.context
        request.produce = sub.produce
        request.notifyResult = sub.printOutput

        return request
    }

    private TaskEntry createTaskEntry( CmdSub command, def index = null, List<DataRef> variables =null ) {

        // -- create a new ID for this task
        final id = dataStore.nextTaskId()

        // create a new request object
        final request = createReq(command)

        // -- define some context environment variables
        request.environment['JOB_ID'] = command.ticket.toString()
        request.environment['TASK_ID'] = id.toString()

        // -- update the context
        if ( variables ) {
            // create a copy of context obj
            request.context = Context.copy(command.context)

            // add the variable to the context
            variables.each { request.context.put(it) }
        }

        // add the task index
        if ( index ) {
            request.environment['TASK_INDEX'] = index.toString()
        }


        final entry = new TaskEntry(id, request )
        entry.sender = new WorkerRef(getSender())
        entry.status = TaskStatus.NEW

        return entry
    }

    /**
     * Reply to a command 'node'
     *
     * @param command
     */
    private void handleCmdNode( CmdNode command ) {

        def reply = new NodeReply(command.ticket)

        /*
         * apply the PAUSE for the specified nodes
         */
        if( command.pause ) {

            try {
                tellToNodes( command.pause, new PauseWorker( hard: command.hard ) )
            }
            catch( Exception e ) {
                log.error("Cannot pause nodes: ${command.pause}", e )
                reply.error << e.getMessage()
            }

        }

        /*
         * apply the 'RESUME' for the specified nodes
         */
        else if ( command.resume ) {

            try {
                tellToNodes( command.resume, new ResumeWorker() )
            }
            catch( Exception e ) {
                reply.error << e.getMessage()
            }

        }

        /*
         * just return the 'stats' for the nodes
         */
        else {
            reply.nodes = dataStore.listNodes()
        }


        getSender().tell(reply, getSelf())
    }


    private tellToNodes ( List<String> nodes, def message ) {
        assert message

        List<Address> targetNodes

        /*
         * fetch all the address
         */
        if ( nodes == ['ALL'] ) {
            targetNodes = allNodesAddresses()
        }

        /*
         * Translate the user entered string node address to real address object
         * When bad nodes are entered do not apply teh operation
         */
        else {
            targetNodes = stringToAddress( nodes )
        }

        targetNodes.each { Address address ->
            def actor = getContext().system().actorFor("${address}/user/${NodeMaster.ACTOR_NAME}")
            actor.tell( message, getSelf() )
        }

    }



    private List<Address> stringToAddress( List<String> nodes ) {

        def allAddresses = allNodesAddresses()
        def result = []
        nodes.each { stringAddress ->

            def addr = allAddresses.find { Address it -> CircoHelper.fmt(it) == stringAddress }
            if ( !addr ) {
                throw new IllegalArgumentException("Unknown node: '$stringAddress' -- command ignored")
            }

            result << addr
        }

        return result
    }

    private List<Address> allNodesAddresses() {
        dataStore.listNodes()?.collect { NodeData node -> node.address } ?: []
    }




}