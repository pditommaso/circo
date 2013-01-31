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
class TaskIdTest extends Specification {

    def 'test constructor' () {

        when:
        def job1 = new TaskId(1)
        def job2 = new TaskId(15)
        def job3 = new TaskId('1')
        def job4 = new TaskId('f')

        then:
        job1 == job3
        job2 == job4
        job1 != job2
    }

    def 'test copy' () {

        when:
        def job1 = new TaskId(1)
        def job2 = new TaskId(16)

        then:
        TaskId.copy(job1) == job1
        TaskId.copy(job2) == job2
        TaskId.copy(job1) != job2

    }


    def 'test equalsAndHash' () {

        expect:
        new TaskId(9) == TaskId.of(9)
        new TaskId(10) == TaskId.of('a')
        new TaskId(10) != TaskId.of('b')
        new TaskId('1234') == TaskId.of('1234')
        new TaskId('1234') != TaskId.of('1235')

        TaskId.of('1234').hashCode() == TaskId.of('1234').hashCode()
        TaskId.of('1234').hashCode() != TaskId.of('1235').hashCode()
    }


    def 'test compareTo' () {

        expect:
        TaskId.of('100') < TaskId.of('101')
        TaskId.of('200') > TaskId.of('101')
        TaskId.of(8) > TaskId.of(5)
        TaskId.of(256) > TaskId.of('ff')
        TaskId.of('fe') < TaskId.of('ff')
    }

    def 'test toString' () {

        when:
        def id1 = new TaskId(1)
        def id2 = new TaskId('f')

        then:
        id1.toString() == '1'
        id2.toString() == 'f'

    }

    def 'test toFmtString' () {

        when:
        def id1 = new TaskId(1)
        def id2 = new TaskId(255)

        then:
        id1.toFmtString() == '1'
        id2.toFmtString() == '0ff'

    }

}
