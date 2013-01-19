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

package circo.frontend

import spock.lang.Specification

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class AbstractCmdResultTest extends Specification {

    def testMessages() {

        when:
        def cmd = new AbstractResponse('123') {}
        cmd.info("info 1")
        cmd.warn("warn 1")
        cmd.warn("warn 2")
        cmd.error("error 1")
        cmd.error("error 2")
        cmd.error("error 3")


        then:
        cmd.messages.get(AbstractResponse.Level.INFO) == ['info 1']
        cmd.messages.get(AbstractResponse.Level.WARN) == ['warn 1', 'warn 2']
        cmd.messages.get(AbstractResponse.Level.ERROR) == ['error 1', 'error 2', 'error 3']


    }

    def 'test getter' () {
        when:
        def cmd0 = new AbstractResponse('111') {}
        def cmd1 = new AbstractResponse('222') {}
        cmd1.info << 'ciao'
        def cmd2 = new AbstractResponse('333') {}
        cmd2.warn << 'hola'

        def cmd3 = new AbstractResponse('444') {}
        cmd3.error << 'hi'

        then:
        cmd0.ticket == '111'
        !cmd0.hasMessages()
        !cmd0.hasInfo()
        !cmd0.hasWarn()
        !cmd0.hasError()

        cmd1.hasMessages()
        cmd1.hasInfo()
        !cmd1.hasWarn()
        !cmd1.hasError()

        cmd2.hasMessages()
        !cmd2.hasInfo()
        cmd2.hasWarn()
        !cmd2.hasError()

        cmd3.hasMessages()
        !cmd3.hasInfo()
        !cmd3.hasWarn()
        cmd3.hasError()

    }
}
