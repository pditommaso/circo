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
import groovy.transform.ToString
/**
 *
 *  @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */

@EqualsAndHashCode
@ToString(includes=['jobId','exitCode','success'], includePackage = false)
class JobResult implements Serializable {

    /** The Job of this job */
    JobId jobId

    /** The exitcode as returned by the system */
    int exitCode

    /** The program output */
    String output

    /** Complete successfully of with error */
    boolean success

    /** The exception raised, in any */
    Throwable failure


}
