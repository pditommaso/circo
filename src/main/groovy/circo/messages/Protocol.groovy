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
import akka.actor.ActorRef
import circo.model.TaskId
import circo.model.TaskResult
import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString
import groovy.transform.TupleConstructor
import circo.model.WorkerRef

/**
 *
 *  @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */

@EqualsAndHashCode
@ToString(includePackage = false)
class WorkerCreated implements Serializable {

    final WorkerRef worker

    def WorkerCreated( WorkerRef worker ) {
        this.worker = worker
    }

    def WorkerCreated( ActorRef actor ) {
        this.worker = new WorkerRef(actor)
    }
}

@EqualsAndHashCode
@ToString(includePackage = false)
class WorkerRequestsWork implements Serializable {

    final WorkerRef worker

    def WorkerRequestsWork( WorkerRef worker ) {
        this.worker = worker
    }

    def WorkerRequestsWork( ActorRef actor ) {
        this.worker = new WorkerRef(actor)
    }

}

@EqualsAndHashCode
@TupleConstructor
@ToString(includePackage = false, includeNames = true)
class WorkIsDone implements Serializable {

    final WorkerRef worker

    final TaskId taskId

}

@EqualsAndHashCode
@TupleConstructor
@ToString(includePackage = false, includeNames = true)
class WorkToBeDone implements Serializable {

    final TaskId taskId

}


@Singleton
@ToString(includePackage = false, includeNames = true)
class WorkIsReady implements  Serializable { }


@TupleConstructor
@EqualsAndHashCode
@ToString(includePackage = false)
class WorkComplete implements Serializable {

    final TaskResult result

}

@TupleConstructor
@EqualsAndHashCode
@ToString(includePackage = false, includeNames = true)
class WorkToSpool implements Serializable {

    final TaskId taskId

    static WorkToSpool of( TaskId taskId )  { new WorkToSpool(taskId) }

}

@TupleConstructor
@EqualsAndHashCode
@ToString(includePackage = false)
class WorkerFailure implements Serializable {

    final WorkerRef worker

    def WorkerFailure( WorkerRef worker ) {
        this.worker = worker
    }

    def WorkerFailure( ActorRef actor ) {
        this.worker = new WorkerRef(actor)
    }

}

@EqualsAndHashCode
@ToString(includePackage = false, includeNames = true)
class PauseWorker implements Serializable {

    /** Hard pause -- all running jobs will be killed */
    boolean hard

}

@EqualsAndHashCode
@ToString(includePackage = false)
class ResumeWorker implements Serializable {

}



