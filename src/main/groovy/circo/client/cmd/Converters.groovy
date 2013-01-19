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

import com.beust.jcommander.IStringConverter
import groovy.transform.ToString
import groovy.util.logging.Slf4j
import circo.messages.JobStatus
import scala.concurrent.duration.Duration


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
class JobStatusArrayConverter implements IStringConverter<JobStatus[]> {

    @Override
    JobStatus[] convert(String value) {
        log.debug "Converting value '$value' to a Duration JobStatusArray"

        if ( !value ) return []

        value.split(',').collect { JobStatus.fromString( it.toUpperCase() ) }.unique().toArray() as JobStatus[]
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
class IntRangeConverter implements IStringConverter<Range<Integer>> {

    @Override
    Range<Integer> convert(final String value) {
        log.debug "Converting value '$value' to a IntRange"
        if ( !value ) return null

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
            result = new IntRangeSerializable(min,max)
        }

        if ( !result && (pos = str.indexOf('-')) != -1 ) {
            def min = Integer.parseInt(str.substring(0,pos))
            def max = Integer.parseInt(str.substring(pos+1))
            result = new IntRangeSerializable(min,max)
        }

        if ( !result ) {
            def val = Integer.parseInt(str)
            if ( val > 0 ) {
                result = new IntRangeSerializable(1,val)
            }
            else {
                throw new IllegalArgumentException("Not a valid range value: $str")
            }
        }

        result.step(step)
        return result
    }

}


@Slf4j
@ToString(includes = 'from,to,step', includePackage = false)
class IntRangeSerializable implements Serializable {

    @Delegate
    transient private IntRange target

    private int step = 1

    IntRangeSerializable(int from, int to, int step = 1 ) {
        target = new IntRange(from, to)
        this.step = step
        if ( this.step != 1 ) {
            target.step(step)
        }
    }

    def List<Integer> step(int step) {
        this.step = step
        target.step(step)
    }

    def void step( int step, Closure callback ) {
        throw new IllegalAccessException('Method not supported')
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
        out.writeInt(target.getFromInt())
        out.writeInt(target.getToInt())
        out.writeInt(step)
    }

    /**
     * Reconstitute the <tt>ArrayList</tt> instance from a stream (that is,
     * deserialize it).
     */
    private void readObject(java.io.ObjectInputStream input) throws java.io.IOException, ClassNotFoundException {

        def from = input.readInt()
        def to = input.readInt()
        this.step = input.readInt()


        this.target = new IntRange(from,to)
        if ( step != 1 ) {
            target.step(step)
        }

    }
}
