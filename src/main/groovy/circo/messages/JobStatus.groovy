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

/**
 * Model the execution status of a job
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
enum  JobStatus {

    VOID('-'),       // no status defined - when it is created
    NEW('N'),        // the jobs has been acquired by the system
    PENDING('P'),     // the jobs has been added to a working queue
    READY('A'),      // the job is ready to be executed
    RUNNING('R'),    // the job execution has been launched
    COMPLETE('C'),   // complete successfully
    FAILED('E')      // terminated with error


    private shortNotation;

    def JobStatus( def val ) {
        this.shortNotation = val
    }

    def String toFmtString() { shortNotation }

    static def JobStatus fromString(String value) {
        assert value

        def result = JobStatus.values().find { it.toString() == value }
        if ( result ) {
            return result
        }

        switch(value) {
            case '-': return VOID
            case 'N': return NEW
            case 'P': return PENDING
            case 'A': return READY
            case "R": return RUNNING
            case 'C': return COMPLETE
            case 'E': return FAILED
            default:
                throw new IllegalArgumentException("String '$value' is not a valid ${JobStatus.simpleName} value")
        }

    }

}
