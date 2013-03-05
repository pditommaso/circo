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
import circo.model.TaskId
import circo.util.SerializeId
import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString
import groovy.transform.TupleConstructor
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@SerializeId
@TupleConstructor
@EqualsAndHashCode
@ToString(includePackage = false, includeNames = true)
class ProcessStarted implements Serializable {

    final TaskEntry task

}


@SerializeId
@EqualsAndHashCode
@ToString(includePackage = false, includeNames = true)
class ProcessKill implements Serializable {

    /**
     * Whenever the job is killed by a user requested 'pause' operation
     */
    boolean cancel

    /**
     * The ID of the task to kill
     */
    TaskId taskId

}


@SerializeId
@Singleton
@ToString(includePackage = false, includeNames = true)
class ProcessIsAlive implements Serializable {

}

@SerializeId
@TupleConstructor
@EqualsAndHashCode
@ToString(includePackage = false, includeNames = true)
class ProcessToRun implements Serializable {

    final TaskEntry task

}

@SerializeId
@ToString(includePackage = false)
class NodeShutdown implements Serializable {

}

@SerializeId
@TupleConstructor
@ToString(includePackage = false, includeNames = true)
class ProcessJobComplete implements  Serializable {

    /**
     * The task entry that completes the overall job
     */
    TaskEntry task

}

