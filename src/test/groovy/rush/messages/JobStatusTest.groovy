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

package rush.messages

import spock.lang.Specification

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class JobStatusTest extends Specification {


    def "test equals" () {
        expect:
        JobStatus.VOID == JobStatus.VOID
        JobStatus.COMPLETE == JobStatus.COMPLETE
        JobStatus.FAILED == JobStatus.FAILED
        JobStatus.RUNNING == JobStatus.RUNNING
        JobStatus.RUNNING != JobStatus.PENDING
    }

    def "test toFmtString" () {

        expect:
        str == status.toFmtString()

        where:
        str   | status
        '-'   | JobStatus.VOID
        'R'   | JobStatus.RUNNING
        'A'   | JobStatus.READY
        'C'   | JobStatus.COMPLETE
        'E'   | JobStatus.FAILED
        'N'   | JobStatus.NEW
        'P'   | JobStatus.PENDING
    }

    def "test fromString" () {
        expect:
        JobStatus.fromString(str) == status

        where:
        str       | status
        '-'       | JobStatus.VOID
        'VOID'    | JobStatus.VOID
        'R'       | JobStatus.RUNNING
        'RUNNING' | JobStatus.RUNNING
        'A'       | JobStatus.READY
        'READY'   | JobStatus.READY
        'C'       | JobStatus.COMPLETE
        'COMPLETE' | JobStatus.COMPLETE
        'E'       | JobStatus.FAILED
        'FAILED'  | JobStatus.FAILED
        'N'       | JobStatus.NEW
        'NEW'     | JobStatus.NEW
        'P'       | JobStatus.PENDING
        'PENDING'  | JobStatus.PENDING
    }


    def "test fromString invalid" () {
        when:
        JobStatus.fromString('X')

        then:
        thrown(IllegalArgumentException)

    }

    def "test valueOf"  (){

        expect:
        Enum.valueOf(JobStatus,'COMPLETE') == JobStatus.COMPLETE

    }
}
