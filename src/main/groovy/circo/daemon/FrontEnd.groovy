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
import circo.client.CmdList
import circo.client.CmdNode
import circo.client.CmdStat
import circo.client.CmdSub
import circo.data.DataStore
import circo.messages.PauseWorker
import circo.messages.ResumeWorker
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
import circo.reply.ListReply
import circo.reply.NodeReply
import circo.reply.ResultReply
import circo.reply.StatReply
import circo.reply.SubReply
import circo.util.CircoHelper
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

    def FrontEnd( DataStore store, int nodeId = 0 ) {
        this.dataStore = store
        this.nodeId = nodeId
    }

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


    {
        dispatchTable[ CmdSub ] = this.&handleCmdSub
        dispatchTable[ CmdStat ] = this.&handleCmdStat
        dispatchTable[ CmdNode ] = this.&handleCmdNode
        dispatchTable[ CmdList ] = this.&handleCmdList
        //dispatchTable[ CmdGet ] = this.&handleCmdGet
    }


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

    void handleCmdList( CmdList command ) {
        assert command

        /*
         *  Look for all the jobs
         */
        def jobs = dataStore.findAllJobs().collect { new ListReply.JobInfo(it) }
        jobs.each {  ListReply.JobInfo it ->

            // set the command
            // TODO ++ this must change - the TaskReq have to become JobReq
            it.command = dataStore.findTasksByRequestId(it.requestId).find()?.req?.script

            // find all failed tasks
            if( it.failed ) {
                it.failedTasks = dataStore.findTasksByRequestId(it.requestId).findAll { TaskEntry entry -> entry.failed }
            }

        }

        /*
         * create the reply object
         */

        def reply = new ListReply(command.ticket)
        reply.jobs = jobs

        sender().tell(reply, self())

    }


    void handleCmdStat(CmdStat command) {
        assert command

        def result = new StatReply(command.ticket)
        List<TaskEntry> list = null

        /*
         * Return all TaskEntry which IDs have been specified by teh command
         */
        if( command.jobs ) {

            list = new LinkedList<>()
            log.debug "Get job info for ${command.jobs}"

            command.jobs.each { String it ->
                try {
                    def found = dataStore.findTasksById(it)
                    if( found ) {
                        list.addAll( found )
                    }
                    else {
                        result.warn "Cannot find any job for id: '${it}'"
                    }
                }
                catch( Exception e ) {
                    log.error "Unable to find info for job with id: '${it}'"
                    result.error( e.getMessage() )
                }
            }

        }

        /*
         * find by status
         */
        else if ( command.status ) {
            try {
                list = dataStore.findTasksByStatusString( command.status )
            }
            catch( Exception e ) {
                log.error "Unable to retrieve tasks for status: '${command.status}'", e
                result.error << e.getMessage()
            }
        }

        /*
         * The list of all the jobs (!)
         */
        else if( command.all ) {
            list = dataStore.findAllTasks()
        }

        /*
         * return some stats
         */
        else {
            result.stats = dataStore.findTasksStat()
        }


        /*
         * reply back to the sender
         */
        result.tasks = list

        log.debug "-> ${result} TO sender: ${sender}"
        getSender().tell( result, getSelf() )

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
            log.debug "sub each: ${command.eachItems}"
            def index=0
            command.context.combinations( command.eachItems ) { List<DataRef> variables ->
                log.debug "Variables combination: ${variables}"
                def entry = createTaskEntry(command, index++, variables)
                listOfTasks << entry
            }
        }

        else {
            listOfTasks << createTaskEntry( command )
        }


        /*
         * reply to the sender with TaskId assigned to the received JobRequest
         */
        if ( getSender()?.path()?.name() != 'deadLetters' ) {
            def result = new SubReply(command.ticket)
            result.taskIds = listOfTasks .collect { it.id }
            log.debug "Send confirmation to client: $result"
            getSender().tell( result, getSelf() )
        }

        /*
         * When the jobs are save, the data-store will trigger one event for each of them
         * that will raise the computation
         * Note: the save MUST be after the client notification, otherwise may happen
         * that some jobs terminate (and the termination notification is sent) before the SubReply
         * is sent to teh client, which will cause a 'dead-lock'
         */
        job.status = JobStatus.SUBMITTED
        job.missingTasks.addAll( listOfTasks *. getId() )
        job.input = command.context ? Context.copy(command.context) : new Context()
        job.sender = new WorkerRef(getSender())
        job.numOfTasks = listOfTasks.size()

        dataStore.putJob(job)

        listOfTasks.each { dataStore.saveTask(it) }

    }

    /**
     * @return Convert this job submit command to a valid {@code TaskReq} instance
     */
    private TaskReq createReq(CmdSub sub) {
        assert sub.ticket
        assert sub.command

        def request = new TaskReq()
        request.ticket = sub.ticket
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
        request.environment['TASK_ID'] = id.toFmtString()

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
            reply.nodes = dataStore.findAllNodesData()
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
        dataStore.findAllNodesData()?.collect { NodeData node -> node.address } ?: []
    }


}