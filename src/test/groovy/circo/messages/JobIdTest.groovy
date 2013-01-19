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

    def 'test equalsAndHash' () {

        expect:
        new JobId('1234') == JobId.of('1234')
        new JobId('1234') != JobId.of('1235')
        new JobId('1234',99) == JobId.of('1234',99)
        new JobId('1234',99) != JobId.of('1234',98)

        JobId.of('1234').hashCode() == JobId.of('1234').hashCode()
        JobId.of('1234').hashCode() != JobId.of('1235').hashCode()
        JobId.of('1234',32).hashCode() == JobId.of('1234',32).hashCode()
        JobId.of('1234',32).hashCode() != JobId.of('1234',33).hashCode()
    }


    def 'test compareTo' () {

        expect:
        JobId.of('100') < JobId.of('101')
        JobId.of('100',99) < JobId.of('101',11)
        JobId.of('100',7) < JobId.of('100',8)
        JobId.of('200') > JobId.of('101')

    }

    def 'test toString' () {

        when:
        def uuid = UUID.randomUUID()
        def id1 = new JobId(uuid.toString())
        def id2 = new JobId(uuid, 88)

        then:
        id1.toString() == uuid.toString()
        id2.toString() == uuid.toString() + ":88"


    }

    def 'test getTicket' () {

        when:
        def uuid = UUID.randomUUID()
        def jobid = new JobId(uuid)

        then:
        jobid.getTicket() == uuid.toString()


    }

    def 'test FromString' () {

        expect:
        new JobId('112233')  == JobId.fromString('112233')
        new JobId('112233','89')  == JobId.fromString('112233:89')
        new JobId('112233',89)  == JobId.fromString('112233:89')
    }

    def 'test toFmtString' () {
        //TODO
    }


}
