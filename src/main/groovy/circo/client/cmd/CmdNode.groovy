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
import circo.client.ClientApp
import circo.data.NodeData
import circo.frontend.CmdNodeResponse
/**
 * Manage cluster nodes
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@ToString(includePackage = false, includeSuper = true, includeNames = true)
@Parameters(commandNames='node', commandDescription='List nodes available in the cluster')
class CmdNode extends AbstractCommand {

    @Parameter(names='--pause', description="The list of nodes to pause, enter 'ALL' to pause all cluster nodes")
    List<String> pause

    @Parameter(names='--HARD', description = "Used with the 'pause' force all current jobs to stop immediately")
    boolean hard

    @Parameter(names='--resume', description="Resume the computation in the listed nodes, use 'ALL' to resume all cluster nodes")
    List<String> resume

    @Override
    void execute(ClientApp client) {

        if( pause && resume ) {
            throw new IllegalArgumentException("Cannot be specified '--pause' and '--resume' together")
        }

        // send the command and wait for a reply
        CmdNodeResponse response = client.send( this )

        if ( !response ) {
            log.error "Oops! Missing response object -- command aborted"
            return
        }

        response.printMessages()

        if ( response.nodes ) {
            printNodes( response.nodes )
        }
        else if ( (pause || resume) && !response.hasMessages()) {
            println "done"
        }

    }


    static void printNodes( List<NodeData> nodes ) {
        assert nodes != null

        println """\
        address     status  up time  procs  jobs
        -----------------------------------------"""
        .stripIndent()

        if ( !nodes ) { println "--"; return }

        nodes.each { NodeData node ->

            println node.toFmtString()

        }


    }
}
