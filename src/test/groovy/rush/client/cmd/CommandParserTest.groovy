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
import com.beust.jcommander.JCommander
import spock.lang.Specification
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class CommandParserTest extends Specification {

    def 'test parseArgs' () {

        setup:
        def opt = new AppOptions()
        def comm = new JCommander(opt)
        comm.addCommand( new CmdSub() )
        comm.addCommand( new CmdStat() )
        comm.addCommand( new CmdNode() )

        when:
        def parser1 = CommandParser.parse('--help')
        def parser2 = CommandParser.parse('sub', '--max-attempts', '5', '--sync', 'yes' )
        def parser3 = CommandParser.parse('sub', '-h')

        def parser4 = CommandParser.parse('sub', '-xyz' )

        def parser5 = CommandParser.parse('suq', '-x43')

        then:

        !parser1.hasFailure()
        parser1.isHelp()

        !parser2.isHelp()
        !parser2.hasFailure()
        parser2.getCommand() instanceof CmdSub
        (parser2.getCommand() as CmdSub) .maxAttempts == 5
        (parser2.getCommand() as CmdSub) .sync == true

        parser3.isHelp()
        parser3.getCommand() instanceof CmdSub
        !parser3.hasFailure()

        !parser4.isHelp()
        parser4.hasFailure()
        parser4.getFailureMessage() .startsWith(
                """
                Unknown option: -xyz

                Usage: sub [options]
                """.stripIndent().trim()

        )

        parser5.getFailureMessage().trim() ==
                """
                Expected a command, got suq

                Did you mean this?   sub
                """.stripIndent().trim()


    }
}
