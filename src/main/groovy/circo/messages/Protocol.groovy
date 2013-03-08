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
import circo.model.TaskEntry
import circo.model.TaskId
import circo.model.TaskResult
import circo.util.SerializeId
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

    /**
     * The task id that which terminate
     */
    final TaskId taskId

    /**
     * The {@code WorkerRef} that managed to complete the work
     */
    final WorkerRef worker


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

@EqualsAndHashCode
@ToString(includePackage = false, includeNames = true)
class WorkToSpool implements Serializable {

    final List<TaskId> taskId

    WorkToSpool( TaskId taskId ) {
        this([taskId])
    }

    WorkToSpool( Collection<TaskId> listOfTasks ) {
        this.taskId = new ArrayList<>(listOfTasks)
    }

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


@SerializeId
@TupleConstructor
@ToString(includePackage = false, includeNames = true)
class JobFinalize implements  Serializable {

    /**
     * The task entry that completes the overall job
     */
    TaskEntry task

}



