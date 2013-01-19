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

package circo.utils

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Deprecated
class CmdLineParser {

    final String usage

    @Lazy
    CliBuilder cli = {

        def builder = new CliBuilder(usage: usage)
        builder._(longOpt: 'port', argName:'port',  'TCP port to be used by this node instance')
        builder.S(longOpt: 'server', argName:'hostname', 'Ip address or hostname for this instance')
        builder.i(longOpt: 'interactive', args: 0, 'Run this instance using the interactive mode')
        builder.p(longOpt: 'processors', argName: 'num', args:  1, 'Number of processors to use')
        builder._(longOpt: 'debug', 'Run in debug mode')
        builder._(longOpt: 'trace', 'Run in trace mode')
        builder.h(longOpt: 'help', 'This help')

        return builder
    } ()


    def CmdLineParser(String usage) {
        this.usage = usage
    }


    def parse(String... args) {
        cli.parse(args)
    }

    def usage() {
        cli.usage()
    }


}
