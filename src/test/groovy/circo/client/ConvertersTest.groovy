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

package circo.client

import circo.model.TaskStatus
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
        new JobStatusArrayConverter().convert('R') == [ TaskStatus.RUNNING ]
        new JobStatusArrayConverter().convert('r') == [ TaskStatus.RUNNING ]
        new JobStatusArrayConverter().convert('R,T') == [ TaskStatus.RUNNING, TaskStatus.TERMINATED  ]
        new JobStatusArrayConverter().convert('R,T,R,T') == [ TaskStatus.RUNNING, TaskStatus.TERMINATED ]

    }

    def 'test CommaSeparatedList' () {
        expect:
        new CommaSeparatedListConverter().convert(null) == null
        new CommaSeparatedListConverter().convert('') == null
        new CommaSeparatedListConverter().convert('a') == ['a']
        new CommaSeparatedListConverter().convert('a,b b,c') == ['a','b b', 'c']
    }

    def 'test IntRange serialize/de-serialize' () {

        when:
        CustomIntRange range1 = new CustomIntRange( 1, 10 )
        CustomIntRange range2 = new CustomIntRange( 3, 33, 5 )
        CustomIntRange range3 = new CustomIntRange( 1, 10, 5 )

        CustomIntRange copy1 = SerializationUtils.clone(range1) as CustomIntRange
        CustomIntRange copy2 = SerializationUtils.clone(range2) as CustomIntRange

        then:
        range1 == copy1
        range2 == copy2
        range1 != range2

    }

    def 'test StringRange serialize/de-serialize' () {

        when:
        CustomStringRange range1 = new CustomStringRange( 'b', 'e' )
        CustomStringRange range2 = new CustomStringRange( 'a', 'f', 2 )

        CustomStringRange copy1 = SerializationUtils.clone(range1) as CustomStringRange
        CustomStringRange copy2 = SerializationUtils.clone(range2) as CustomStringRange

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

    def 'test each list converter' () {

        expect:
        new EachListConverter().convert('a') == ['a']
        new EachListConverter().convert('a,b,c') == ['a','b','c']
        new EachListConverter().convert('a,b=1,c=2..8') == ['a','b=1','c=2..8']
        new EachListConverter().convert('x,y=[1,2,3],z=[a,b]') == ['x','y=[1,2,3]','z=[a,b]']
    }


}
