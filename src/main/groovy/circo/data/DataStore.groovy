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

package circo.data
import akka.actor.Address
import circo.model.NodeData
import circo.reply.StatReplyData
import groovy.transform.CompileStatic
import circo.model.TaskEntry
import circo.model.TaskId
import circo.model.TaskStatus

/**
 * Define the operations provided by the data storage system
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@CompileStatic
interface DataStore {


    TaskId nextJobId()

    /**
     * Save a {@code TaskEntry} in the underlying storage implementation
     * @param job A {@code TaskEntry} instance
     * @return {@code true} if {@code job} was created as a new entry, {@code false} when the entry is updated
     */
    boolean saveJob( TaskEntry job )

    /**
     * Get a job with the specified ID
     *
     * @param jobId The ID of the requested {@code TaskEntry}
     * @return The {@code TaskEntry} instance associated with specified ID or {@code null} if the job does not exist
     */
    TaskEntry getJob( TaskId jobId )

    /**
     *
     * @param jobId
     * @return
     */
    List<TaskEntry> findJobsById( String jobId );

    /**
     * @return The list of all defined jobs
     */
    List<TaskEntry> findAll()

    /**
     * Add a new listener closure invoke when a new {@code TaskEntry} in added in the storage
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
    List<TaskEntry> findJobsByStatus( TaskStatus... status )

    /**
     * The job status statistics i.e. how many jobs for each status
     */
    StatReplyData findJobsStats()

    /**
     * Find the {@code TaskEntry} instance by the specified ID, apply the specified closure and save it
     *
     * @param jobId
     * @param closure
     * @return {@code true} if saved successfully, @{Code false} otherwise
     */
    boolean updateJob( TaskId jobId, Closure closure )

    /**
     * @return The count of {@code TaskEntry} stored
     */
    long countJobs()


    NodeData getNodeData( Address address )

    NodeData putNodeData( NodeData nodeData )

    boolean replaceNodeData( NodeData oldValue, NodeData newValue )

    NodeData updateNodeData( NodeData current, Closure closure )

    boolean removeNodeData( NodeData dataToRemove )

    List<NodeData> findAllNodesData()

}
