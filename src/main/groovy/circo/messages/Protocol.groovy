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
import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString
import groovy.transform.TupleConstructor
import circo.data.WorkerRef

/**
 *
 *  @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */

@ToString(includePackage = false)
@EqualsAndHashCode
class WorkerCreated implements Serializable {

    final WorkerRef worker

    def WorkerCreated( WorkerRef worker ) {
        this.worker = worker
    }

    def WorkerCreated( ActorRef actor ) {
        this.worker = new WorkerRef(actor)
    }
}

@ToString(includePackage = false)
@EqualsAndHashCode
class WorkerRequestsWork implements Serializable {

    final WorkerRef worker

    def WorkerRequestsWork( WorkerRef worker ) {
        this.worker = worker
    }

    def WorkerRequestsWork( ActorRef actor ) {
        this.worker = new WorkerRef(actor)
    }

}

@ToString(includePackage = false)
@EqualsAndHashCode
@TupleConstructor
class WorkIsDone implements Serializable {

    final WorkerRef worker

    final JobEntry job

}

@ToString(includePackage = false)
@EqualsAndHashCode
@TupleConstructor
class WorkToBeDone implements Serializable {

    final JobId jobId

}


@ToString(includePackage = false)
@Singleton
class WorkIsReady implements  Serializable { }


@TupleConstructor
@EqualsAndHashCode
@ToString(includePackage = false)
class WorkComplete implements Serializable {

    final JobResult result

}

@ToString(includePackage = false)
@TupleConstructor
@EqualsAndHashCode
class WorkToSpool implements Serializable {

    final JobId JobId

    static WorkToSpool of( JobId jobId )  { new WorkToSpool(jobId) }

}

@ToString(includePackage = false)
@TupleConstructor
@EqualsAndHashCode
class WorkerFailure implements Serializable {

    final WorkerRef worker

    def WorkerFailure( WorkerRef worker ) {
        this.worker = worker
    }

    def WorkerFailure( ActorRef actor ) {
        this.worker = new WorkerRef(actor)
    }

}

@ToString(includePackage = false)
@EqualsAndHashCode
class PauseWorker implements Serializable {

    /** Hard pause -- all running jobs will be killed */
    boolean hard

}

@ToString(includePackage = false)
@EqualsAndHashCode
class ResumeWorker implements Serializable {

}

