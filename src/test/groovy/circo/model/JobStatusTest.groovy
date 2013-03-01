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

package circo.model

import spock.lang.Specification

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class JobStatusTest extends Specification {

    def 'test toString' () {

        expect:
        JobStatus.PENDING.toString() == 'PENDING'
        JobStatus.RUNNING.toString() == 'RUNNING'
        JobStatus.SUCCESS.toString() == 'SUCCESS'
        JobStatus.ERROR.toString() == 'ERROR'

    }


    def 'test fromString' () {

        expect:
        JobStatus.fromString( str ) == status

        where:
        str         | status
        'p'         | JobStatus.PENDING
        'P'         | JobStatus.PENDING
        'pending'   | JobStatus.PENDING
        'r'         | JobStatus.RUNNING
        'R'         | JobStatus.RUNNING
        'Running'   | JobStatus.RUNNING
        'S'         | JobStatus.SUCCESS
        's'         | JobStatus.SUCCESS
        'Success'   | JobStatus.SUCCESS
        'e'         | JobStatus.ERROR
        'E'         | JobStatus.ERROR
        'ERROR'     | JobStatus.ERROR

    }

    def 'test fromString exception ' () {

        when:
        JobStatus.fromString('x')

        then:
        thrown(IllegalArgumentException)
    }

}
