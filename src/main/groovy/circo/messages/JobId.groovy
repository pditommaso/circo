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
import circo.util.SerializeId
import groovy.transform.EqualsAndHashCode
/**
 * Models a Job unique identifier
 * <p>
 *     It is composed by two parts
 *     1) The request unique identifier in the form of a UUID
 *     2) An optional index value, that may be defined by a job-array
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */

@SerializeId
@EqualsAndHashCode
class JobId implements java.io.Serializable, Comparable<JobId> {

    def long value

    def JobId( def value ) {
        assert value
        this.value = value instanceof Number ? value.longValue() : Long.parseLong(value.toString(), 16)
    }

    def JobId( JobId that ) {
        assert that
        new JobId( that.value )
    }

    def static JobId copy( JobId that ) {
        assert that
        new JobId( that.value )
    }

    def String toString() { "JobId(${})" }

    def String toHexString() {
        Long.toHexString(value)
    }

    def String toHexString(Closure closure) {
        closure.call(Long.toHexString(value))
    }

    /**
     * JobId constructor factory helper method
     */
    static def JobId of( def value ) {
        new JobId(value)
    }

    static def JobId fromString( String value ) {
        new JobId(value)
    }

    @Override
    int compareTo(JobId that) {
        def result = this.value <=> that.value
    }


}
