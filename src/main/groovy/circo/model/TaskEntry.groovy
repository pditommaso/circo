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
import akka.actor.ActorRef
import circo.exception.MissingInputFileException
import circo.util.CircoHelper
import circo.util.SerializeId
import groovy.transform.EqualsAndHashCode
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */

@SerializeId
@EqualsAndHashCode(includes = 'id')
class TaskEntry implements Serializable, Comparable<TaskEntry> {

    /**
     * The unique ID for this job
     */
    def final TaskId id

    /**
     * The job status
     */
    def TaskStatus status = TaskStatus.VOID

    /**
     * The request that originated this job entry
     */
    def final TaskReq req

    /**
     * The result of the job
     */
    def TaskResult result

    /**
     * The actor that send this request
     */
    def WorkerRef sender

    /*
     * The path where the job is executed
     */
    def File workDir


    /**
     * The number of time this jobs has tried to be executed
     */
    def int attempts

    /**
     * Number of times this jobs has been cancelled by the user
     */
    def int cancelled

    /**
     * The worker that raised a failure processing the job
     */
    def WorkerRef worker

    /**
     * The job Unix PID
     */
    def Integer pid

    /*
     * The this entry has been created
     */
    final long creationTime = System.currentTimeMillis()

    /**
     * Timestamp when the job has stated
     */
    def long launchTime

    /**
     * Timestamp when the job has completed
     */
    def long completionTime

    def String getCreationTimeFmt() { CircoHelper.getSmartTimeFormat(creationTime) }

    def String getLaunchTimeFmt() { launchTime ? CircoHelper.getSmartTimeFormat(launchTime) : '-' }

    def String getCompletionTimeFmt() { completionTime ? CircoHelper.getSmartTimeFormat(completionTime) : '-' }

    /** The node to which this task belongs */
    def Integer ownerId


    def TaskEntry( TaskId id, TaskReq req ) {
        assert id
        assert req

        this.id = id
        this.req = req
    }

    def static TaskEntry create( def id, Closure closure = null ) {
        def taskId = id instanceof TaskId ? id : new TaskId(String.valueOf(id))
        def result = new TaskEntry( taskId, new TaskReq())
        if ( closure ) closure.call(result);
        return result
    }

    /**
     * Quick constructor, useful for testing purpose
     * @param id
     * @param script
     * @return
     */
    def TaskEntry( def id, String script ) {
        assert id
        this.id = id instanceof TaskId ? id : new TaskId(String.valueOf(id))
        this.req = new TaskReq(script: script)
    }

    def void setSender( WorkerRef sender ) {
        if ( sender && sender.path().name() != 'deadLetters' ) {
            this.sender = sender
        }
        else {
            this.sender = null
        }
    }

    def void setSender( ActorRef actor ) {
        this.setSender( new WorkerRef(actor) )
    }

    def boolean isValidExitCode(int exitCode) {

        req.validExitCode .contains( exitCode )

    }

    /**
     * Conditions to be satisfied to be a success terminated job
     * 1) the result obj exist
     * 2) the job hasn't cancelled by the user
     * 3) the job terminated with a 'valid' exit code, as defined by {@code #isValidExitCode}
     */
    def boolean isSuccess() {
        if ( !result ) return false
        if ( result.cancelled ) return false
        if ( result.failure ) return false
        isValidExitCode(result.exitCode)
    }

    def boolean isFailed() {
        if ( result == null ) return false
        if ( result.cancelled ) return false
        if ( result.failure ) return true

        !isValidExitCode(result.exitCode)
    }

    def boolean isCancelled() {
        result != null && result.cancelled
    }

    def boolean isTerminated() {
       status == TaskStatus.TERMINATED
    }

    def String getTerminatedReason() {
        if( !terminated ) return null
        if( success ) return 'success'
        if( failed ) return 'fail'
        if ( cancelled ) return cancelled
    }


    def boolean isRetryRequired() {
        // -- when terminated successfully no retry by definition
        if ( isSuccess() ) {
            return false
        }

        // -- when failed by missing input do not retry
        if ( result?.failure instanceof MissingInputFileException ) {
            return false
        }

        attempts - cancelled < req.maxAttempts || req.maxAttempts <= 0
    }

    @Override
    int compareTo(TaskEntry that) {
        return this.id <=> that.id
    }


    def void setStatus( TaskStatus status ) {

        this.status = status

        if( !launchTime && status == TaskStatus.RUNNING  ) {
            launchTime = System.currentTimeMillis()
        }
        else if ( !completionTime && status == TaskStatus.TERMINATED ) {
            completionTime = System.currentTimeMillis()
        }

    }

    /**
     * @return The formatted time depending the job status.
     * If the job is {@code #COMPLETE} or {@code #FAILED} returns the {@code #getCompletionTimeFmt},
     * if the job is {@code #RUNNING} retuns the {@code #getLaunchTimeFmt}
     * otherwise return the {@code getCreationTimeFmt}
     */
    def String getStatusTimeFmt() {

        if( status == TaskStatus.TERMINATED ) {
            getCompletionTimeFmt()
        }
        else if( status == TaskStatus.RUNNING ) {
            getLaunchTimeFmt()
        }
        else {
            getCreationTimeFmt()
        }

    }

    /**
     * Note, when a result object is specified, some job properties are modified accordingly the
     * provided result
     *
     * @param result
     */
    def void setResult( TaskResult result ) {
        this.result = result
        if( result ) {
            // increment the number of times this job has been cancelled
            if ( result.cancelled ) this.cancelled++

            if( isSuccess() || !isRetryRequired() ) {
                setStatus(TaskStatus.TERMINATED)
                ownerId = null
            }

        }
    }


    String toString() {

"TaskEntry(id=$id,\
 status=$status,\
 hasResult=${result!=null},\
 exitCode=${result?.exitCode?:'-'},\
 failure=${result?.failure?.getMessage()?:'-'},\
 cancelled=${result?.cancelled?:'-'},\
 attemptTimes=$attempts,\
 cancelledTimes=$cancelled )"

    }

}
