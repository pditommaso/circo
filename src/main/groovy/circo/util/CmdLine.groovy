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

package circo.util

import circo.Consts
import com.beust.jcommander.Parameter
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class CmdLine {

    @Parameter
    def List<String> args

    @Parameter(names=['--port'], description='TCP port to use to run this node instance')
    def int port = Consts.DEFAULT_AKKA_PORT

    @Parameter(names=['-i','--interactive'], description='Run this node in interactive mode, showing stats information')
    def boolean interactive = false

    @Parameter(names=['-p','--cores'], description='Number or processors to use')
    def int processors = Runtime.getRuntime().availableProcessors()

    @Parameter(names=['-h',"--help"], help = true)
    def boolean help;

    @Parameter(names='--debug', variableArity = true)
    def List<String> debug

    @Parameter(names='--trace', variableArity = true)
    def List<String> trace

    @Parameter(names='--local', description='Run in local mode', hidden = true)
    def boolean local

    @Parameter(names='--join', description = 'Join the nodes specified ')
    def String join = "auto"

    @Parameter(names='--host', description = 'Host name/address for this node')
    def String host = InetAddress.getLocalHost().getHostAddress()

    @Parameter(names=['--public-address'] )
    def String publicAddress


    @Parameter(names='--slow', description = 'Add an extra overhead on job execution to simulation slow node', hidden = true)
    def int slow = 0


}
