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
import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString

import java.util.concurrent.TimeUnit
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@ToString
@EqualsAndHashCode
class JobReq implements Serializable {

    /** The script to be executed */
    def String script

    /** The external to use to interpret the script */
    def String shell = 'bash'

    /** The exit code expected for job terminated with no error */
    def List<Integer> validExitCode = [0]

    /** Defines the environment to be used to execute the job */
    def Map<String,String> environment

    /** The maximum amount of time without getting output from the job */
    def long maxInactive

    /**
     * The maximum amount of time (millis) the job can run. If this value is exceeded the process is killed.
     * Set to zero for duration upper bound limit. Default value 5 minutes
     */
    def long maxDuration = TimeUnit.MINUTES.toMillis(5)

    /**
     * The number of tentative before declare the job failed
     */
    def int maxAttempts = 2

    /**
     * The user who submitted the request
     */
    def String user



}
