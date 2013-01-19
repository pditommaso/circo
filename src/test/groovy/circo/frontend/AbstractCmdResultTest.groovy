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
        def cmd = new AbstractResponse() {}
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
}
