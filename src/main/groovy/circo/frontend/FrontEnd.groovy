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
import groovy.util.logging.Slf4j
import circo.JobMaster
import circo.client.cmd.*
import circo.data.DataStore
import circo.data.NodeData
import circo.data.WorkerRef
import circo.messages.*
import circo.utils.RushHelper

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
        dispatchTable[ CmdPause ] = this.&handleCmdPause
        dispatchTable[ CmdNode ] = this.&handleCmdNode
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

    void handleCmdPause(CmdPause message) {

        // send the 'pause' message to all master nodes
        //pause(dataStore.findAllNodesData()?.collect{ NodeData node -> node.address } )
        throw IllegalAccessException('TODO')

        // confirm the operation
        getSender().tell( new CmdAckResponse(message.ticket), getSelf() )
    }

    void handleCmdStat(CmdStat command) {
        assert command

        def result = new CmdStatResponse(command.ticket)
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
    void handleCmdSub(CmdSub cmdSub) {
        assert cmdSub

        final ticket = cmdSub.ticket
        final request = cmdSub.createJobReq()

        /*
         * create a new job request for each times required
         */
        def count = 0
        if( cmdSub.times ) {
            cmdSub.times.each  { index ->

                createAndSaveJobEntry( request, ticket, index, cmdSub.times )
                count++
            }
        }
        else {
            count = 1
            createAndSaveJobEntry( request, ticket )
        }


        /*
         * reply to the sender with JobId assigned to the received JobRequest
         */
        if ( getSender()?.path()?.name() != 'deadLetters' ) {
            log.debug "-> ${ticket} TO sender: ${getSender()}"

            def result = new CmdSubResponse(ticket)
            result.numOfJobs = count

            getSender().tell( result, getSelf() )
        }


    }

    private JobEntry createAndSaveJobEntry( JobReq request, def ticket, def index = null, def range = null ) {
        assert ticket

        final id = new JobId( ticket, index )

        // -- define some context environment variables
        request.environment['JOB_ID'] = id.toString()
        request.environment['JOB_NAME'] = id.ticket?.toString()

        if( index != null && range != null ) {
            // for backward compatibility with SGE use the same variable name for job arrays
            request.environment['SGE_TASK_ID'] = index.toString()
            request.environment['SGE_TASK_FIRST'] = range.from.toString()
            request.environment['SGE_TASK_LAST'] = range.to.toString()
        }

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

        def reply = new CmdNodeResponse(command.ticket)

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

            def addr = allAddresses.find { Address it -> RushHelper.fmt(it) == stringAddress }
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