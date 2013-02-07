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
import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString
/**
 *
 *  @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */

@EqualsAndHashCode
@ToString(includes=['taskId','exitCode'], includePackage = false, includeNames = true)
class TaskResult implements Serializable {

    /** The Job of this job */
    TaskId taskId

    /** The exit-code as returned by the system */
    int exitCode = Integer.MIN_VALUE

    /** The program output */
    String output

    /** The exception raised, in any */
    Throwable failure

    /** Whenever the job terminated by a user 'cancel' request */
    boolean cancelled

    /** The resulting context */
    TaskContext context


}
