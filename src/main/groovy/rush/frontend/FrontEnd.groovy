/*
 * Copyright (c) 2012, the authors.
 *
 *    This file is part of Rush.
 *
 *    Rush is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation, either version 3 of the License, or
 *    (at your option) any later version.
 *
 *    Rush is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with Rush.  If not, see <http://www.gnu.org/licenses/>.
 */



package rush.frontend
import akka.actor.ActorRef
import akka.actor.UntypedActor
import groovy.util.logging.Slf4j
import rush.JobMaster
import rush.client.cmd.CmdStat
import rush.client.cmd.CmdSub
import rush.data.DataStore
import rush.data.WorkerRef
import rush.messages.JobEntry
import rush.messages.JobId
import rush.messages.JobReq
import rush.messages.JobStatus

/**
 *
 *  @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */


@Slf4j
class FrontEnd extends UntypedActor {

    static final ACTOR_NAME = 'FrontEnd'

    def ActorRef master

    def DataStore dataStore

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

    /**
     * Handles the message received by the FrontEnd actor
     *
     * @param message
     */

    @Override
    void onReceive(Object message) {
        log.debug "<- ${message}"

        if( message instanceof CmdSub )  {
            handleCmdSub(message)
        }

        else if( message instanceof CmdStat ) {
            handleCmdJob(message)
        }

        else {
            unhandled(message)
        }

    }

    void handleCmdJob(CmdStat cmd) {
        assert cmd

        def result = new CmdStatResponse(ticket: cmd.ticket)
        List<JobEntry> list = null

        /*
         * Return all JobEntry which IDs have been specified by teh command
         */
        if( cmd.jobs ) {

            list = new LinkedList<>()
            log.debug "Get job info for ${cmd.jobs}"

            cmd.jobs.each { String it ->
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
        else if ( cmd.status ) {
            list = dataStore.findJobsByStatus( cmd.status )
        }

        else if( cmd.all ) {
            list = dataStore.findAll()
        }

        /*
         * Invalid job query
         */
        else {
            log.warn "Invalid job command: $cmd"
            list = []
            result.warn('Invalid job command')
        }

        /*
         * reply back to the sender
         */
        result.jobs = list
        result.success = true

        log.debug "-> ${result} TO sender: ${sender}"
        getSender().tell( result )

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

            def result = new CmdSubResponse(ticket: ticket, success: true, numOfJobs: count)
            getSender().tell( result )
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
}