/*
 * Copyright (c) 2012, the authors.
 *
 *    This file is part of 'Circo'.
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

import circo.util.CircoHelper
import circo.util.SerializeId
import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString

/**
 * Collect results of multiple tasks execution and model the final result of a job request
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@SerializeId
@EqualsAndHashCode
@ToString(includeNames = true, includePackage = false)
class Job implements Serializable {

    /**
     * The request unique identifier
     */
    final UUID requestId

    /**
     * @return The first 8 characters of the request id
     */
    String getShortReqId() { requestId?.toString()?.substring(0,8) }

    /**
     * The sender actor to which reply the {@code JobReply} instance
     */
    WorkerRef sender

    /**
     * The final job status
     */
    JobStatus status

    /**
     * Number of tasks that made up the job
     */
    int numOfTasks

    /**
     * The input context for this task
     */
    Context input

    /**
     * The result is expressed by a new context
     */
    Context output

    /**
     * When this job has been created
     */
    final long creationTime = System.currentTimeMillis()

    /**
     * When the job has been completed
     */
    long completionTime

    def String getCreationTimeFmt() { CircoHelper.getSmartTimeFormat(creationTime) }

    def String getCompletionTimeFmt() { completionTime ? CircoHelper.getSmartTimeFormat(completionTime) : '-' }


    def Job( UUID id ) {
        assert id
        this.requestId = id
    }

    def static Job create( UUID requestId, Closure closure = null ) {

        def result = new Job(requestId)
        if ( closure ) closure.call(result)
        return result

    }

    def static Job create( Closure callback = null ) {
        create(UUID.randomUUID(),callback)
    }


    boolean isSubmitted() {  status == JobStatus.PENDING  }

    boolean isSuccess() { status == JobStatus.SUCCESS }

    boolean isFailed() { status == JobStatus.ERROR  }

    boolean isRunning() { status == JobStatus.RUNNING }

    boolean isKilled() { status == JobStatus.KILLED }

    static final private TERMINATED = [JobStatus.ERROR, JobStatus.SUCCESS ]

    void setStatus( JobStatus status ) {

        // guard condition -- avoid to update 'completionTime' when status has not changed
        if( this.status == status ) return

        this.status = status
        if( status in TERMINATED ) {
            this.completionTime = System.currentTimeMillis()
        }

    }

    /**
     * @param that The instance to be copied 
     */
    private Job( Job that ) {

        this.requestId = that.requestId
        this.sender = that.sender ? WorkerRef.copy(that.sender) : null
        this.status = that.status
        this.numOfTasks = that.numOfTasks
        this.input = that.input ? Context.copy( that.input ) : null
        this.output = that.output ? Context.copy( that.output ) : null
        this.creationTime = that.creationTime
        this.completionTime = that.completionTime
    }
    
    
    /**
     * Makes a copy of this collector
     *
     * @param that The instance to be copied
     * @return
     */
    static def Job copy( Job that ) {
        assert that
        return new Job(that)
    }

}
