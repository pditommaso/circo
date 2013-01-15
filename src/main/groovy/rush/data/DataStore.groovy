/*
 * Copyright (c) 2012, the authors.
 *
 *    This file is part of Rush.
 *
 *    Rush is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation, either version 3 of the License, or
 *    (at your option) any later version.
 *
 *    Rush is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with Rush.  If not, see <http://www.gnu.org/licenses/>.
 */

package rush.data
import akka.actor.Address
import groovy.transform.CompileStatic
import rush.messages.JobEntry
import rush.messages.JobId
import rush.messages.JobStatus

/**
 * Define the operations provided by the data storage system
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@CompileStatic
interface DataStore {

    /**
     * Save a {@code JobEntry} in the underlying storage implementation
     * @param job A {@code JobEntry} instance
     * @return {@code true} if {@code job} was created as a new entry, {@code false} when the entry is updated
     */
    boolean saveJob( JobEntry job )

    /**
     * Get a job with the specified ID
     *
     * @param jobId The ID of the requested {@code JobEntry}
     * @return The {@code JobEntry} instance associated with specified ID or {@code null} if the job does not exist
     */
    JobEntry getJob( JobId jobId )

    /**
     *
     * @param jobId
     * @return
     */
    List<JobEntry> findJobsById( String jobId );

    /**
     * @return The list of all defined jobs
     */
    List<JobEntry> findAll()

    /**
     * Add a new listener closure invoke when a new {@code JobEntry} in added in the storage
     *
     * @param listener
     */
    void addNewJobListener( Closure listener )

    /**
     * Remove the specified instance from the listeners registered
     *
     * @param listener
     */
    void removeNewJobListener( Closure listener )


    /**
     * Find the jobs in the specified status
     *
     * @param status An open array of job status, it CANNOT be empty
     * @return The list of jobs in status specified, or an empty list when no job matches the status specified
     */
    List<JobEntry> findJobsByStatus( JobStatus... status )

    /**
     * Find the {@code JobEntry} instance by the specified ID, apply the specified closure and save it
     *
     * @param jobId
     * @param closure
     * @return {@code true} if saved successfully, @{Code false} otherwise
     */
    boolean updateJob( JobId jobId, Closure closure )

    /**
     * @return The count of {@code JobEntry} stored
     */
    long countJobs()


    NodeData getNodeData( Address address )

    NodeData putNodeData( NodeData nodeData )

    boolean replaceNodeData( NodeData oldValue, NodeData newValue )

    NodeData updateNodeData( NodeData current, Closure closure )

    boolean removeNodeData( NodeData dataToRemove )

    List<NodeData> findAllNodesData()

}
