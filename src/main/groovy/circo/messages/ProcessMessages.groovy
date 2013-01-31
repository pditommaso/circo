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

import circo.model.TaskEntry
import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString
import groovy.transform.TupleConstructor
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@TupleConstructor
@EqualsAndHashCode
@ToString(includePackage = false)
class ProcessStarted implements Serializable {

    final TaskEntry jobEntry

}


@ToString(includePackage = false)
class ProcessKill implements Serializable {

    /** Whenever the job is killed by a user requested 'pause' operation */
    boolean cancel

}


@Singleton
@ToString(includePackage = false)
class ProcessIsAlive implements Serializable {

}

@TupleConstructor
@EqualsAndHashCode
@ToString(includePackage = false)
class ProcessToRun implements Serializable {

    final TaskEntry jobEntry

}
