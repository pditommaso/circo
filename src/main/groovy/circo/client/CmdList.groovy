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

import circo.reply.ListReply
import circo.ui.TableBuilder
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

    @Parameter(names='dump', hidden = true)
    boolean dump

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
        if( dump ) {
            log.info reply.dump()
            return
        }


        if( !reply.jobs )  {
            log.debug "No jobs to list"
            return
        }

        /*
         * render the table
         */
        def table = new TableBuilder()
                .head('id',9)
                .head('command', 25)
                .head('status')
                .head('creat')
                .head('compl')
                .head('C')
                .head('F')
                .head('T')

        reply.jobs?.sort { ListReply.JobInfo it -> it.completionTime }?.each {

            table << it.requestId
            table << it.command
            table << it.status?.toString() ?: '-'
            table << it.creationTimeFmt
            table << it.completionTimeFmt
            table << ( it.numOfTasks - it.missingTasks?.size() )
            table << ( it.failedTasks?.size())
            table << ( it.numOfTasks )

            table.closeRow()

        }


        println table.toString()


    }




}
