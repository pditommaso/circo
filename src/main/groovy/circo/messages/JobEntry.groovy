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
import circo.data.WorkerRef
import circo.util.CircoHelper
import circo.util.SerializeId

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */

@SerializeId
@EqualsAndHashCode(includes = 'id')
@ToString(includes = 'id', includePackage = false)
class JobEntry implements Serializable, Comparable<JobEntry> {

    /**
     * The job status
     */
    def JobStatus status = JobStatus.VOID

    /**
     * The unique ID for this job
     */
    def final JobId id

    /**
     * The request that originated this job entry
     */
    def final JobReq req

    /**
     * The result of the job
     */
    def JobResult result

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

    /** Number of times this jobs has been cancelled by the user */
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

    def JobEntry( JobId id, JobReq req ) {
        assert id
        assert req

        this.id = id
        this.req = req
    }

    def static JobEntry create( def id, Closure closure = null ) {
        def jobId = id instanceof JobId ? id : JobId.of(id)
        def result = new JobEntry( jobId, new JobReq())
        if ( closure ) closure.call(result);
        return result
    }

    /**
     * Quick constructor, useful for testing purpose
     * @param id
     * @param script
     * @return
     */
    def JobEntry( def id, String script ) {
        assert id
        this.id = id instanceof JobId ? id : new JobId(String.valueOf(id))
        this.req = new JobReq(script: script)
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
        result != null && !result.cancelled && isValidExitCode(result.exitCode)
    }

    def boolean isFailed() {
        result != null && !result.cancelled && !isValidExitCode(result.exitCode)
    }

    def boolean isCancelled() {
        result != null && result.cancelled
    }


    def boolean retryIsRequired() {
        (attempts-cancelled < req.maxAttempts || req.maxAttempts <= 0) && !isSuccess()
    }

    @Override
    int compareTo(JobEntry that) {
        return this.id <=> that.id
    }


    def void setStatus( JobStatus status ) {

        this.status = status

        if( !launchTime && status == JobStatus.RUNNING  ) {
            launchTime = System.currentTimeMillis()
        }
        else if ( !completionTime && status in [ JobStatus.COMPLETE, JobStatus.FAILED ]) {
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

        if( status in [ JobStatus.COMPLETE, JobStatus.FAILED ] ) {
            getCompletionTimeFmt()
        }
        else if( status == JobStatus.RUNNING ) {
            getLaunchTimeFmt()
        }
        else {
            getCreationTimeFmt()
        }

    }

    /**
     * Note, when a resutl object is specified, some job properties are modified accordingly the
     * provided result
     *
     * @param result
     */
    def void setResult( JobResult result ) {
        this.result = result
        if( result ) {
            // increment the number of times this job has been cancelled
            if ( result.cancelled ) this.cancelled++

            if( isSuccess() ) {
                setStatus(JobStatus.COMPLETE)
            }
            else if ( !retryIsRequired() ) {
                setStatus(JobStatus.FAILED)
            }

        }
    }

}
