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
class TaskId implements java.io.Serializable, Comparable<TaskId> {

    def int value

    def TaskId( def value ) {
        assert value != null
        this.value = value instanceof Number ? value.intValue() : Integer.parseInt(value.toString(), 16)
    }

    def TaskId( TaskId that ) {
        assert that != null
        new TaskId( that.value )
    }

    def static TaskId copy( TaskId that ) {
        assert that
        new TaskId( that.value )
    }

    def String toString() { Integer.toHexString(value) }

    def String toFmtString() {
        def result = Integer.toHexString(value)
        return result.charAt(0).isLetter() ? '0'+result : result
    }

    def String toFmtString(Closure closure) {
        closure.call(Integer.toHexString(value))
    }

    /**
     * TaskId constructor factory helper method
     */
    static def TaskId of( def value ) {
        new TaskId(value)
    }

    static def TaskId fromString( String value ) {
        new TaskId(value)
    }

    @Override
    int compareTo(TaskId that) {
        def result = this.value <=> that.value
    }


}
