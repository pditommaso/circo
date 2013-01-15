/*
 * Copyright (c) 2012, the authors.
 *
 *    This file is part of Rush.
 *
 *    Rush is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation, either version 3 of the License, or
 *    (at your option) any later version.
 *
 *    Rush is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with Rush.  If not, see <http://www.gnu.org/licenses/>.
 */

package rush.messages
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

@EqualsAndHashCode
class JobId implements Serializable, Comparable<JobId> {

    def String ticket

    def index

    def JobId( def ticket, def index = null ) {
        assert ticket
        this.ticket = ticket.toString()
        this.index = index
    }


    def String toString() {
        def result = ticket
        if ( index ) {
            result += ':' + index
        }

        result
    }

    def String getIndexStr() { index?.toString() ?: null }

    def String toFmtString() {

        def result = ticket?.size()>8 ? ticket.substring(0,8) : ticket

        if ( index ) {
            result += ':' + index
        }

        result

    }

    /**
     * JobId constructor factory helper method
     */
    static def JobId of( def ticket, def index = null ) {
        assert ticket
        new JobId(ticket.toString(),index)
    }

    @Override
    int compareTo(JobId that) {
        def result = this.ticket <=> that.ticket
        if ( result == 0 ) {
            result = this.index <=> that.index
        }
        return result
    }


}
