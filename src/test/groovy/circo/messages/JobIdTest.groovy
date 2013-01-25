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

package circo.messages

import spock.lang.Specification

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class JobIdTest extends Specification {

    def 'test constructor' () {

        when:
        def job1 = new JobId(1)
        def job2 = new JobId(15)
        def job3 = new JobId('1')
        def job4 = new JobId('f')

        then:
        job1 == job3
        job2 == job4
        job1 != job2
    }

    def 'test copy' () {

        when:
        def job1 = new JobId(1)
        def job2 = new JobId(16)

        then:
        JobId.copy(job1) == job1
        JobId.copy(job2) == job2
        JobId.copy(job1) != job2

    }


    def 'test equalsAndHash' () {

        expect:
        new JobId(9) == JobId.of(9)
        new JobId(10) == JobId.of('a')
        new JobId(10) != JobId.of('b')
        new JobId('1234') == JobId.of('1234')
        new JobId('1234') != JobId.of('1235')

        JobId.of('1234').hashCode() == JobId.of('1234').hashCode()
        JobId.of('1234').hashCode() != JobId.of('1235').hashCode()
    }


    def 'test compareTo' () {

        expect:
        JobId.of('100') < JobId.of('101')
        JobId.of('200') > JobId.of('101')
        JobId.of(8) > JobId.of(5)
        JobId.of(256) > JobId.of('ff')
        JobId.of('fe') < JobId.of('ff')
    }

    def 'test toString' () {

        when:
        def id1 = new JobId(1)
        def id2 = new JobId('f')

        then:
        id1.toString() == 'JobId(1)'
        id2.toString() == 'JobId(f)'

    }

    def 'test toHexString' () {

        when:
        def id1 = new JobId(1)
        def id2 = new JobId(255)

        then:
        id1.toHexString() == '1'
        id2.toHexString() == 'ff'

    }

}
