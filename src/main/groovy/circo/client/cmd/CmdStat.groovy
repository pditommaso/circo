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

package circo.client.cmd

import com.beust.jcommander.Parameter
import com.beust.jcommander.Parameters
import groovy.transform.ToString
import groovy.util.logging.Slf4j
import org.apache.commons.lang.exception.ExceptionUtils
import circo.client.ClientApp
import circo.data.JobsStat
import circo.reply.StatReply
import circo.messages.JobEntry
import circo.messages.JobStatus

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

        log.debug "Sending $this"
        StatReply result = client.send(this)

        if( !result ) {
            log.error "Unknown error -- missing result object"
            return
        }

        result.printMessages()

        if( result.stats ) {
            printStats(result.stats)
        }
        else if( this.jobs ) {
            printJobsDetails( result.jobs )
        }
        else {
            printJobsTable( result.jobs )
        }


    }

    def static void printStats(JobsStat stats) {

        println """
        cluster status
        --------------
        pending : ${ stats[ JobStatus.PENDING ].toString().padLeft(4) }
        running : ${ stats[ JobStatus.RUNNING].toString().padLeft(4)  }
        complete: ${ stats[ JobStatus.COMPLETE].toString().padLeft(4)  }
        failed  : ${ stats[ JobStatus.FAILED].toString().padLeft(4)  }
        """
        .stripIndent()
    }

    def static void printJobsDetails( List<JobEntry> jobs )  {

        log.debug "Print details for jobs: ${jobs}"

        jobs .eachWithIndex {  JobEntry job, index ->

            if ( index>0 ) {
                println "------------"
            }

            def entry = """
            id        : ${job.id.toFmtString()}
            command   : ${job.req?.script?:'-'}
            get       : ${job.req?.get?:'-'}
            produce   : ${job.req?.produce?:'-'}
            status    : ${job.status}
            sender    : ${job.sender?.toFmtString()}
            worker    : ${job.worker?.toFmtString()}
            tmpdir    : ${job.workDir}
            linux pid : ${job.pid}
            attempts  : ${job.attempts}
            user      : ${job?.req?.user}
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
