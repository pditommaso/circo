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

package circo.model

import spock.lang.Specification
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class TaskStatusTest extends Specification {


    def "test equals" () {
        expect:
        TaskStatus.VOID == TaskStatus.VOID
        TaskStatus.TERMINATED == TaskStatus.TERMINATED
        TaskStatus.RUNNING == TaskStatus.RUNNING
        TaskStatus.RUNNING != TaskStatus.PENDING
    }

    def "test toFmtString" () {

        expect:
        str == status.toFmtString()

        where:
        str   | status
        '-'   | TaskStatus.VOID
        'R'   | TaskStatus.RUNNING
        'A'   | TaskStatus.READY
        'T'   | TaskStatus.TERMINATED
        'N'   | TaskStatus.NEW
        'P'   | TaskStatus.PENDING
    }

    def "test fromString" () {
        expect:
        TaskStatus.fromString(str) == status

        where:
        str       | status
        '-'       | TaskStatus.VOID
        'VOID'    | TaskStatus.VOID
        'R'       | TaskStatus.RUNNING
        'RUNNING' | TaskStatus.RUNNING
        'A'       | TaskStatus.READY
        'READY'   | TaskStatus.READY
        'T'       | TaskStatus.TERMINATED
        'TERMINATED' | TaskStatus.TERMINATED
        'N'       | TaskStatus.NEW
        'NEW'     | TaskStatus.NEW
        'P'       | TaskStatus.PENDING
        'PENDING'  | TaskStatus.PENDING
        'p'       | TaskStatus.PENDING
        'Pending'  | TaskStatus.PENDING
    }


    def "test fromString invalid" () {
        when:
        TaskStatus.fromString('X')

        then:
        thrown(IllegalArgumentException)

    }

    def "test valueOf"  (){

        expect:
        Enum.valueOf(TaskStatus,'TERMINATED') == TaskStatus.TERMINATED

    }
}
