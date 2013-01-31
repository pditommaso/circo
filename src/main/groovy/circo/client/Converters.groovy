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
import circo.util.CircoHelper
import circo.util.SerializeId
import com.beust.jcommander.IStringConverter
import groovy.util.logging.Slf4j
import scala.concurrent.duration.Duration

import static circo.Const.LIST_CLOSE_BRACKET
import static circo.Const.LIST_OPEN_BRACKET

class CommaSeparatedListConverter implements  IStringConverter<List<String>>  {

    @Override
    List<String> convert(String value) {
        if ( !value ) return null

        return value.split(',')
    }
}

/**
 * Used by JCommander to convert a string value to a duration
 */
@Slf4j
class JobStatusArrayConverter implements IStringConverter<TaskStatus[]> {

    @Override
    TaskStatus[] convert(String value) {
        log.debug "Converting value '$value' to a Duration JobStatusArray"

        if ( !value ) return []

        value.split(',').collect { TaskStatus.fromString( it.toUpperCase() ) }.unique().toArray() as TaskStatus[]
    }
}

/**
 * Used by JCommander to convert a string value to a {@code Duration} object
 */
@Slf4j
class DurationConverter implements IStringConverter<Duration> {
    @Override
    Duration convert(String value) {
        log.debug "Converting value '$value' to a Duration object"
        return Duration.create(value);
    }
}

/**
 * Converts a times value from the string notation to a {@code Range} object
 * <p>
 *      Accepted syntax is
 *      <pre>
 *      1-100   i.e. from 1 to 100
 *      0-9:2   i.e. from 0 to 9 with a step of two
 *      </pre>
 */
@Slf4j
class IntRangeConverter implements IStringConverter<CustomIntRange> {

    @Override
    CustomIntRange convert(final String value) {
        log.trace "Converting value '$value' to a IntRange"
        if ( !value ) {
            return null
        }

        def str = value
        int step = 1
        def result = null

        // parse the step value -- if provided
        def pos = value.indexOf(':')
        if( pos != -1 ) {
            step = Integer.parseInt(str.substring(pos+1))
            str = str.substring(0,pos)
        }

        // range can be separated by a '-' or '..'
        pos = str.indexOf('..')
        if( pos != -1 ) {
            def min = Integer.parseInt(str.substring(0,pos))
            def max = Integer.parseInt(str.substring(pos+2))
            result = new CustomIntRange(min,max,step)
        }

        if ( !result && (pos = str.indexOf('-')) != -1 ) {
            def min = Integer.parseInt(str.substring(0,pos))
            def max = Integer.parseInt(str.substring(pos+1))
            result = new CustomIntRange(min,max,step)
        }

        if ( !result ) {
            def val = Integer.parseInt(str)
            if ( val > 0 ) {
                result = new CustomIntRange(1,val,step)
            }
            else {
                throw new IllegalArgumentException("Not a valid range value: $str")
            }
        }

        return result

    }

}


abstract class AbstractCustomRange<T extends Comparable> implements Range<T>, Serializable {

    @Delegate
    protected transient Range<T> target

    protected int $step = 1

    AbstractCustomRange(Range<T> target, int step) {
        this.target = target
        this.$step = step
    }


    def List<T> step(int step) {
        target.step(step)
    }

    def List<T> withStep() {
        return target.step($step)
    }

    def void step( int step, Closure callback ) {
        throw new IllegalAccessException('Method not supported')
    }

    def String toString() {
        def result = "${from}..${to}"
        if ( $step != 1 ) result += ':' + $step
        return result
    }

}


@Slf4j
@SerializeId
class CustomIntRange extends AbstractCustomRange<Integer> {

    CustomIntRange(int from, int to, int step = 1 ) {
        super(new IntRange(from, to), step)
    }

    /**
     * Save the state of the <tt>ArrayList</tt> instance to a stream (that
     * is, serialize it).
     *
     * @serialData The length of the array backing the <tt>ArrayList</tt>
     *             instance is emitted (int), followed by all of its elements
     *             (each an <tt>Object</tt>) in the proper order.
     */
    private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException
    {
        out.writeInt(getFrom() as int)
        out.writeInt(getTo() as int)
        out.writeInt($step)
    }

    /**
     * Reconstitute the <tt>ArrayList</tt> instance from a stream (that is,
     * deserialize it).
     */
    private void readObject(java.io.ObjectInputStream input) throws java.io.IOException, ClassNotFoundException {

        def from = input.readInt()
        def to = input.readInt()
        this.$step = input.readInt()
        this.target = new IntRange(from,to)

    }


}


@Slf4j
class CustomStringRange extends AbstractCustomRange<String> {

    CustomStringRange(String from, String to, int step = 1 ) {
        super(new ObjectRange(from, to), step)
    }


    /**
     * Save the state of the <tt>ArrayList</tt> instance to a stream (that
     * is, serialize it).
     *
     * @serialData The length of the array backing the <tt>ArrayList</tt>
     *             instance is emitted (int), followed by all of its elements
     *             (each an <tt>Object</tt>) in the proper order.
     */
    private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException
    {
        out.writeObject(getFrom())
        out.writeObject(getTo())
        out.writeInt($step)
    }

    /**
     * Reconstitute the <tt>ArrayList</tt> instance from a stream (that is,
     * de-serialize it).
     */
    private void readObject(java.io.ObjectInputStream input) throws java.io.IOException, ClassNotFoundException {

        def from = input.readObject()
        def to = input.readObject()
        this.$step = input.readInt()
        this.target = new ObjectRange(from?.toString(),to?.toString())
    }


}

@Deprecated
class EachConverter implements IStringConverter<List> {

    @Override
    List convert(String value) {

        if ( !value ) return []

        if( value.contains(',') && value.contains('..') ) { throw new IllegalArgumentException("Specify either a collection (comma separated) or a range") }

        int p = value.indexOf('..')
        if ( p != -1 ) {
            return CircoHelper.parseRange(value)
        }
        else {
            return value.split(',')?.collect { it?.trim() } as List<String>
        }

    }
}

class EachListConverter implements IStringConverter<List>  {


    @Override
    List convert(String value) {

        List<String> result = []
        StringTokenizer tokenizer = new StringTokenizer(value,',',true)
        while( tokenizer.hasMoreTokens() ) {
            String current = tokenizer.nextToken()
            if ( current == ',') {
                // do not care
            }
            else if ( current .contains(LIST_OPEN_BRACKET) ) {
                consumeListValue(current,tokenizer, result)
            }
            else {
                result << current
            }
        }

        result
    }

    String consumeListValue( String current, StringTokenizer tokenizer, List<String> appender ) {

        String remain = null
        def result = new StringBuilder(current)

        while( tokenizer.hasMoreTokens() ) {
            String tkn = tokenizer.nextElement()
            int p = tkn.indexOf(LIST_CLOSE_BRACKET)
            if( p != -1 ) {
                result << tkn.substring(0,p+1)
                remain = tkn.substring(p+1)
                break
            }
            else {
                result << tkn
            }

        }

        appender << result.toString()
        if ( remain ) {
            appender << remain
        }
    }


}
