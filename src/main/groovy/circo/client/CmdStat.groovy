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

package circo.client
import circo.model.TaskEntry
import circo.reply.StatReply
import circo.reply.StatReplyData
import com.beust.jcommander.Parameter
import com.beust.jcommander.Parameters
import groovy.transform.ToString
import groovy.util.logging.Slf4j
import org.apache.commons.lang.exception.ExceptionUtils
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@ToString(includePackage = false)
@Parameters(commandNames='stat', commandDescription = 'Display jobs information')
class CmdStat extends AbstractCommand {

    @Parameter(names=['-j','--job'], description = 'Show detailed information about the specified job-id(s)')
    List<String> jobs

    @Parameter(names=['-s','--status'], description = 'Comma separated list of job status (Pending|Running|Terminated|Success|Failed)' )
    String status

    /** returns all jobs */
    @Parameter(names=['-a','--all'], hidden = true)
    boolean all

    /**
     * Run the 'job' command i.e. summit the request to the server, wait for the result and print out the
     * server reply
     *
     * @param client The client application instance
     */
    @Override
    void execute(ClientApp client) {

        log.debug "Sending $this"
        StatReply result = client.send(this)

        if( !result ) {
            log.error "Unknown error -- missing result object"
            return
        }

        // messages returned by the server
        result.printMessages()

        if( result.stats ) {
            printStats(result.stats)
        }
        else if( this.jobs ) {
            printTasksDetails( result.tasks )
        }
        else {
            printJobsTable( result.tasks )
        }


    }

    def static void printStats(StatReplyData stats) {

        println """
        cluster status
        --------------
        pending: ${ stats.pending.toString().padLeft(4) }
        running: ${ stats.running.toString().padLeft(4)  }
        success: ${ stats.successful.toString().padLeft(4)  }
        failed : ${ stats.failed.toString().padLeft(4) }
        """
        .stripIndent()
    }

    def static void printTasksDetails( List<TaskEntry> jobs )  {

        log.debug "Print details for jobs: ${jobs}"

        jobs .eachWithIndex {  TaskEntry entry, index ->

            if ( index>0 ) {
                println "------------"
            }

            def result = """
            id       : ${entry.id.toFmtString()}
            command  : ${entry.req?.script?:'-'}
            produce  : ${entry.req?.produce?:'-'}
            status   : ${entry.status} ${entry.terminated ? "- " + entry.terminatedReason : ''}
            sender   : ${entry.sender?.toFmtString()}
            worker   : ${entry.worker?.toFmtString()}
            owner    : ${entry.ownerId}
            tmpdir   : ${entry.workDir}
            linux pid: ${entry.pid}
            attempts : ${entry.attempts}
            user     : ${entry?.req?.user}
            created  : ${entry.getCreationTimeFmt()}
            launched : ${entry.getLaunchTimeFmt()}
            completed: ${entry.getCompletionTimeFmt()}
            exit code: ${entry.result?.exitCode?.toString() ?: '-'}
            failure  : ${entry.result?.failure ? '\n'+ExceptionUtils.getStackTrace(entry.result?.failure) : '-' }
            """
            .stripIndent()

            println result

        }


    }


    /**
     * print the result table header
     */
    static void printHead() {
        println """\
        job-id        state  submit/start    worker
        -------------------------------------------------------\
        """
        .stripIndent()
    }

    /**
     * Print out the list of retrieved jobs in the text formatted table
     *
     */
    static void printJobsTable( List<TaskEntry> tasks ) {

        printHead()

        if( !tasks ) {
            println "   -           -      -               -"
        }

        tasks?.sort { it.creationTime } ?.each { TaskEntry it ->

            final String id = it.id.toFmtString()
            final state = it.status.toFmtString() + ( it.terminatedReason?.substring(0,1) ?: '' )
            final String timestamp = it.getStatusTimeFmt()
            final String worker = (!it.terminated && it.worker) ? it.worker.toFmtString() : '-'

            println "${id.padRight(12)}   ${state.padRight(4)}  ${timestamp.padRight(10)}  ${worker}"

        }

    }

    def static string


}
