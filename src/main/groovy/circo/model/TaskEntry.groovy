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
import groovy.transform.ToString
import groovy.util.logging.Slf4j

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */

@Slf4j
@SerializeId
@EqualsAndHashCode
@ToString(includePackage = false, includeNames = true)
class TaskEntry implements Serializable {

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
    def int attemptsCount

    /**
     * Number of times this jobs has been cancelled by the user
     */
    def int cancelCount

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

    /**
     * The node to which this task belongs
     */
    def Integer ownerId

    /**
     * Whenever the job execution has been killed
     */
    def boolean killed

    def boolean isAborted() {
        killed || isCancelled()
    }

    def void setKilled( boolean value ) {
        this.killed = value
        if ( killed ) {
            setStatus(TaskStatus.TERMINATED)
        }
    }


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
        if ( killed ) return false
        isValidExitCode(result.exitCode)
    }

    def boolean isFailed() {
        if ( result == null ) return false
        if ( result.cancelled ) return false
        if ( result.failure ) return true
        if ( killed ) return false

        !isValidExitCode(result.exitCode)
    }

    def boolean isCancelled() {
        result != null && result.cancelled
    }

    def boolean isTerminated() {
       status == TaskStatus.TERMINATED
    }

    def boolean isRunning() {
        status == TaskStatus.RUNNING
    }

    def boolean isNew() {
        status == TaskStatus.NEW
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

        // -- when then task has been kill, no retry any more
        if( killed ) {
            return false
        }

        attemptsCount - cancelCount < req.maxAttempts || req.maxAttempts <= 0
    }


    def void setStatus( TaskStatus status ) {

        this.status = status

        if( status == TaskStatus.TERMINATED )  {
            ownerId = null
        }

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

    def String getStatusString() {
        if( status == TaskStatus.TERMINATED ) {
            def result
            if ( isKilled() ) {
                result = 'KILLED'
            }
            else if ( isCancelled() ) {
                result = 'CANCELLED'
            }
            else if ( isFailed() ) {
                result = 'ERROR'
            }
            else if ( isSuccess() ) {
               result = 'SUCCESS'
            }
            else {
                log.debug "Unknown task status: ${status} -- ${this.dump()}"
                result = null
            }
            return result
        }

        return status.toString()
    }

    /**
     * Note, when a result object is specified, some job properties are modified accordingly the
     * provided result
     *
     * @param result
     */
    def void setResult( TaskResult result ) {
        this.result = result
        if( result && status != TaskStatus.TERMINATED ) {

            // increment the number of times this job has been cancelled
            if ( result.cancelled ) this.cancelCount++

            if( isSuccess() || !isRetryRequired() ) {
                setStatus(TaskStatus.TERMINATED)
            }

        }
    }

    def int getFailedCount() {
        attemptsCount>0 ? (attemptsCount - cancelCount -1) : 0
    }



}
