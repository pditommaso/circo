/*
 * Copyright (c) 2012, the authors.
 *
 *    This file is part of 'Circo'.
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
import circo.reply.ListReply
import circo.ui.TableBuilder
import circo.ui.TextLabel
import com.beust.jcommander.Parameter
import com.beust.jcommander.Parameters
import groovy.transform.ToString
import groovy.util.logging.Slf4j
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@ToString(includeNames = true, includePackage = false)
@Parameters(commandNames = 'list', commandDescription = 'List the current jobs in the grid')
class CmdList extends AbstractCommand {

    @Parameter
    List<String> jobsId

    @Parameter(names=['-t','--task'], description = 'Show detailed information about the specified task(s)')
    boolean tasks

    @Parameter(names=['-s','--status'], description = 'Comma separated list of job status (Pending|Running|Terminated|Success|Failed)' )
    List<String> status

    /**
     * returns all jobs
     */
    @Parameter(names=['-a','--all'], hidden = true)
    boolean all

    @Parameter(names=['-l'], description = 'Use long notation for job id(s)')
    boolean longId

    @Override
    void execute(ClientApp client) throws IllegalArgumentException {

        ListReply reply = client.send(this)

        if( !reply )  {
            log.error "Oops.. missing reply!"
            return
        }

        /*
         * dump the result
         */
        if( dumpFlag ) {
            log.info reply.dump()
            return
        }


        if( reply.jobs )  {
            printJobs( reply.jobs )
        }

        else if ( reply.tasks ) {
            printTasks(reply.tasks)
        }

        else {
            log.debug "(empty)"
        }


    }


    void printJobs(List<ListReply.JobInfo> jobs ) {


        /*
         * render the table
         */
        def table = new TableBuilder()
                .head('id')
                .head('command', 25)
                .head('status')
                .head('creat')
                .head('compl')
                .head('C')
                .head('F')
                .head('T')

        jobs?.sort { ListReply.JobInfo it -> it.completionTime }?.each {

            table << (longId ? it.requestId?.toString() : it.shortReqId)
            table << it.command
            table << it.status?.toString() ?: '-'
            table << it.creationTimeFmt
            table << it.completionTimeFmt
            table << ( it.numOfTasks - it.numOfPendingTasks )
            table << ( it.numOfFailedTasks )
            table << ( it.numOfTasks )

            table.closeRow()

        }


        println table.toString()

    }


    void printTasks( List<TaskEntry> tasks ) {

        def table = new TableBuilder()
            .head('job')
            .head('task')
            .head('status')
            .head('time')
            .head('node', TextLabel.Align.LEFT)
            .head('attps', TextLabel.Align.LEFT)
            .head('pid', TextLabel.Align.LEFT)
            .head('exit', TextLabel.Align.LEFT)
            .head('fail', 25)


        tasks.sort { TaskEntry entry -> entry.id } .each { TaskEntry task ->

            table << (longId ? task.req?.requestId?.toString() : task.req?.shortReqId)
            table << task.id.toString()
            table << task.statusString
            table << task.statusTimeFmt
            table << task.ownerId
            table << task.attemptsCount
            table << task.pid
            table << task.result?.exitCode
            table << task.result?.failure?.toString()
            table.closeRow()

        }


        println table.toString()
    }




}
