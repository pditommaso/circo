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

import scala.concurrent.duration.Duration
import spock.lang.Specification
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class CmdSubTest extends Specification {


    def 'test parse cmd sub ' () {

        when:
        def times
        def parser = CommandParser.parse( 'sub -t 1-10 --max-duration 7min --max-inactive 8sec --sync echo hello world' )
        CmdSub cmd = parser.getCommand()


        then:
        !parser.hasFailure()
        !parser.isHelp()
        cmd.maxDuration == Duration.create('7 minutes')
        cmd.maxInactive == Duration.create('8 seconds')
        cmd.times == new IntRange(1,10)
        cmd.command.join(' ') == 'echo hello world'

    }

    def 'test parse cmd sub times' () {

        when:
        def times
        def parser = CommandParser.parse( 'sub --times 1-10:2' )
        CmdSub cmd = parser.getCommand()

        then:
        !parser.hasFailure()
        !parser.isHelp()
        cmd.times == new IntRange(1,10)

    }

    def 'test parse cmd sub each' () {

        when:
        def times
        def parser = CommandParser.parse( 'sub --each a,b,c' )
        CmdSub cmd = parser.getCommand()


        then:
        !parser.hasFailure()
        !parser.isHelp()
        cmd.eachItems == ['a','b','c']

    }



}
