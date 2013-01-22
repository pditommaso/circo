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

import circo.messages.JobStatus
import org.apache.commons.lang.SerializationUtils
import spock.lang.Specification

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class ConvertersTest extends Specification {



    def 'test times converted' () {

        expect:
        new IntRangeConverter().convert(times).withStep() == range

        where:
        times       | range
        '1-3'       | range(1,3)
        '0-3'       | range(0,3)
        '1..2'      | range(1,2)
        '5'         | range(1,5)
        '6:2'       | range(1,6,2)
        '1-100'     | range(1,100)
        '22-54'     | range(22,54)
        '0-9:2'     | range(0,9,2)
        '33-66:15'  | range(33,66,15)

    }

    def 'test times illegal' () {
        when:
        new IntRangeConverter().convert('a-b')
        then:
        thrown(IllegalArgumentException)
    }

    def 'test times illegal 2' () {
        when:
        new IntRangeConverter().convert('-1-4')
        then:
        thrown(IllegalArgumentException)
    }

    static List<String> range( int min, int max, int step = 1 ) {
        def range = new IntRange(min,max)
        range.step(step)
    }

    def 'test StatusConverter' () {

        expect:
        new JobStatusArrayConverter().convert(null) == []
        new JobStatusArrayConverter().convert('R') == [ JobStatus.RUNNING ]
        new JobStatusArrayConverter().convert('r') == [ JobStatus.RUNNING ]
        new JobStatusArrayConverter().convert('R,C,e') == [ JobStatus.RUNNING, JobStatus.COMPLETE, JobStatus.FAILED ]
        new JobStatusArrayConverter().convert('R,C,R,C') == [ JobStatus.RUNNING, JobStatus.COMPLETE ]

    }

    def 'test CommaSeparatedList' () {
        expect:
        new CommaSeparatedListConverter().convert(null) == null
        new CommaSeparatedListConverter().convert('') == null
        new CommaSeparatedListConverter().convert('a') == ['a']
        new CommaSeparatedListConverter().convert('a,b b,c') == ['a','b b', 'c']
    }

    def 'test IntRange serialize-deserialize' () {

        when:
        IntRangeSerializable range1 = new IntRangeSerializable( 1, 10 )
        IntRangeSerializable range2 = new IntRangeSerializable( 3, 33, 5 )
        IntRangeSerializable range3 = new IntRangeSerializable( 1, 10, 5 )

        IntRangeSerializable copy1 = SerializationUtils.clone(range1) as IntRangeSerializable
        IntRangeSerializable copy2 = SerializationUtils.clone(range2) as IntRangeSerializable

        then:
        range1 == copy1
        range2 == copy2
        range1 != range2

    }

    def 'test StringRange serialize-deserialize' () {

        when:
        StringRangeSerializable range1 = new StringRangeSerializable( 'b', 'e' )
        StringRangeSerializable range2 = new StringRangeSerializable( 'a', 'f', 2 )

        StringRangeSerializable copy1 = SerializationUtils.clone(range1) as StringRangeSerializable
        StringRangeSerializable copy2 = SerializationUtils.clone(range2) as StringRangeSerializable

        then:
        range1 == copy1
        range2 == copy2
        range1 != range2
        range2.withStep() == ['a','c','e']
    }

    def 'test each converter' () {
        expect:
        new EachConverter().convert(str) == list

        where:
        str         |  list
        null        | []
        ''          | []
        'a'         | ['a']
        'a, b, z'   | ['a','b','z']
        'b..f'      | ['b','c','d','e','f']
        '1..4'      | [1,2,3,4]

    }


}
