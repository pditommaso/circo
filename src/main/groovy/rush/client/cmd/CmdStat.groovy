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

package rush.client.cmd

import com.beust.jcommander.Parameter
import com.beust.jcommander.Parameters
import groovy.transform.ToString
import groovy.util.logging.Slf4j
import rush.client.ClientApp
import rush.frontend.CmdStatResponse
import rush.messages.JobEntry
import rush.messages.JobStatus
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

    @Parameter(names=['-s','--status'], description = 'Comma separated list of job status (Pending|Running|Complete)', converter = JobStatusArrayConverter)
    JobStatus[] status

    /** returns all jobs */
    @Parameter(names='--all', hidden = true)
    boolean all

    /**
     * Run the 'job' command i.e. summit the request to the server, wait for the result and print out the
     * server reply
     *
     * @param client The client application instance
     */
    @Override
    void execute(ClientApp client) {

        if( !jobs && !status && !all ) {
            status = [ JobStatus.PENDING ]
        }

        log.debug "Sending $this"
        CmdStatResponse result = client.send(this)

        if( !result ) {
            log.error "Unknown error -- missing result object"
            return
        }

        if ( result.hasMessages() ) {
            result.info.each { log.info it }
            result.warn.each { log.warn "${it}" }
            result.error.each { log.error "${it}" }
        }

        if( this.jobs ) {
            showJobsDetails( result.jobs )
        }
        else {
            printJobsTable( result.jobs )
        }


    }

    def static void showJobsDetails( List<JobEntry> jobs )  {

        log.debug "Print details for jobs: ${jobs}"

        jobs .eachWithIndex {  JobEntry job, index ->

            if ( index>0 ) {
                println "------------"
            }

            def entry = """
            id        : ${job.id}
            status    : ${job.status}
            sender    : ${job.sender?.toFmtString()}
            worker    : ${job.worker?.toFmtString()}
            tmpdir    : ${job.workDir}
            linux pid : ${job.pid}
            attempts  : ${job.attempts}
            created   : ${job.getCreationTimeFmt()}
            launched  : ${job.getLaunchTimeFmt()}
            completed : ${job.getCompletionTimeFmt()}
            exit code : ${job.result?.exitCode?.toString() ?: '-'}
            failure   : ${job.result?.failure ? '\n'+ExceptionUtils.getStackTrace(job.result?.failure) : '-' }
            """
            .stripIndent()

            println entry

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
    static void printJobsTable( List<JobEntry> jobs ) {

        printHead()

        if( !jobs ) {
            println "   -           -      -               -"
        }

        jobs?.each { JobEntry job ->

            final String id = job.id.toFmtString()
            final state = job.status.toFmtString()
            final String timestamp = job.getStatusTimeFmt()
            final String worker = job.worker?.toFmtString() ?: '-'

            println "${id.padRight(12)}   ${state.padRight(4)}  ${timestamp.padRight(10)}  ${worker}"

        }

    }

    def static string


}
