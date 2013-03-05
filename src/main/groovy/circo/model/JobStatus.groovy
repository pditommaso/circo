/*
 * Copyright (c) 2012, the authors.
 *
 *    This file is part of 'Circo'.
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

/**
 * Models the possible status of a job
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
enum JobStatus {

    PENDING,
    RUNNING,
    SUCCESS,
    ERROR,
    KILLED


    static JobStatus fromString(String value) {

        def result = JobStatus.values().find { value.equalsIgnoreCase(it.toString()) }
        if ( result ) {
            return result
        }

        switch(value.toUpperCase()) {
            case 'P': return PENDING
            case 'R': return RUNNING
            case 'S': return SUCCESS
            case 'E': return ERROR
            default:
                throw new IllegalArgumentException("String '$value' is not a valid ${JobStatus.simpleName} value")
        }

    }
}
