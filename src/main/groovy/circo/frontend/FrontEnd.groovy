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



package circo.frontend
import akka.actor.ActorRef
import akka.actor.Address
import akka.actor.UntypedActor
import circo.JobMaster
import circo.client.cmd.*
import circo.data.DataRef
import circo.data.DataStore
import circo.data.NodeData
import circo.data.WorkerRef
import circo.messages.*
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
class FrontEnd extends UntypedActor {

    static final ACTOR_NAME = 'FrontEnd'

    def ActorRef master

    def DataStore dataStore

    def Map<Class<? extends AbstractCommand>, Closure> dispatchTable = new HashMap<>()

    def FrontEnd( DataStore store ) {
        this.dataStore = store
    }

    def void preStart() {
        log.debug "++ Starting actor ${getSelf().path()}"

        // if not injected by someone else, get a reference to the 'master' actor -- useful for testing
        if( !master ) {
            master = getContext().system().actorFor("/user/${JobMaster.ACTOR_NAME}")
        }
    }


    def void postStop() {
        log.debug "~~ Stopping actor ${getSelf().path()}"
    }


    {
        dispatchTable[ CmdSub ] = this.&handleCmdSub
        dispatchTable[ CmdStat ] = this.&handleCmdStat
        dispatchTable[ CmdNode ] = this.&handleCmdNode
        dispatchTable[ CmdGet ] = this.&handleCmdGet
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
    void handleCmdGet( CmdGet command ) {
        assert command

        command.listOfIds?.each { String value ->

            def reply = new ResultReply(command.ticket)
            try {
                def entry = dataStore.getJob(JobId.of(value))
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
            getSender().tell(reply, getSelf())
        }
    }



    void handleCmdStat(CmdStat command) {
        assert command

        def result = new StatReply(command.ticket)
        List<JobEntry> list = null

        /*
         * Return all JobEntry which IDs have been specified by teh command
         */
        if( command.jobs ) {

            list = new LinkedList<>()
            log.debug "Get job info for ${command.jobs}"

            command.jobs.each { String it ->
                try {
                    def found = dataStore.findJobsById(it)
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
            list = dataStore.findJobsByStatus( command.status )
        }

        /*
         * The list of all the jobs (!)
         */
        else if( command.all ) {
            list = dataStore.findAll()
        }

        /*
         * return some stats
         */
        else {
            result.stats = dataStore.findJobsStats()
        }


        /*
         * reply back to the sender
         */
        result.jobs = list

        log.debug "-> ${result} TO sender: ${sender}"
        getSender().tell( result, getSelf() )

    }

    /*
     * The client send a 'JobReq' to the coordinator in order to submit a new job request
     */
    void handleCmdSub(CmdSub command) {
        assert command

        /*
         * create a new job request for each times required
         */
        def listOfIds = []
        if( command.times ) {
            command.times.withStep().each  { index ->
                listOfIds << createAndSaveJobEntry( command, index ) .id
            }
        }
        /*
         * Create a job for each entry in the 'eachList'
         */
        else if ( command.getEachItems() ) {
            def index=0
            command.context.combinations( command.eachItems ) { List<DataRef> variables ->
                listOfIds << createAndSaveJobEntry(command, index++, variables) .id
            }

        }
        else {
            listOfIds << createAndSaveJobEntry( command ) .id
        }


        /*
         * reply to the sender with JobId assigned to the received JobRequest
         */
        if ( getSender()?.path()?.name() != 'deadLetters' ) {
            log.debug "-> ${command} TO sender: ${getSender()}"

            def result = new SubReply(command.ticket)
            result.jobIds = listOfIds
            getSender().tell( result, getSelf() )
        }


    }

    /**
     * @return Convert this job submit command to a valid {@code JobReq} instance
     */
    private JobReq createJobReq(CmdSub sub) {
        assert sub.ticket
        assert sub.command

        def request = new JobReq()
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
        request.receive = sub.receive
        request.produce = sub.produce

        return request
    }

    private JobEntry createAndSaveJobEntry( CmdSub command, def index = null, List<DataRef> variables =null ) {

        // -- create a new ID for this job
        final id = dataStore.nextJobId()

        // create a new request object
        final request = createJobReq(command)

        // -- define some context environment variables
        request.environment['JOB_ID'] = id.toFmtString()

        // -- update the context
        if ( variables ) {
            // make sure the 'receive' is not null
            if ( !request.receive ) request.receive = []
            // create a copy of context obj
            request.context = JobContext.copy(command.context)

            variables.each {
                // 1 - add the variable to the context
                request.context.add( it )

                // 2 - the variable contribute to the 'receive' declaration by default
                if ( !request.receive.contains(it.name ) ) {
                    request.receive << it.name
                }
            }
        }

        // add the job index
        if ( index ) {
            request.environment['JOB_INDEX'] = index.toString()
        }

//        if( index != null && range != null ) {
//            // for backward compatibility with SGE use the same variable name for job arrays
//            request.environment['SGE_TASK_ID'] = index.toString()
//            request.environment['SGE_TASK_FIRST'] = range.from.toString()
//            request.environment['SGE_TASK_LAST'] = range.to.toString()
//        }

        final entry = new JobEntry(id, request )
        entry.sender = new WorkerRef(getSender())
        entry.status = JobStatus.NEW
        dataStore.saveJob( entry )

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
            def actor = getContext().system().actorFor("${address}/user/${JobMaster.ACTOR_NAME}")
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