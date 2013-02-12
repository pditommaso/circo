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
import circo.model.Job
import circo.model.NodeData
import circo.model.NodeStatus
import circo.model.TaskEntry
import circo.model.TaskId
import circo.model.TaskStatus
import circo.reply.StatReplyData
import groovy.transform.CompileStatic
/**
 * Define the operations provided by the data storage system
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@CompileStatic
interface DataStore {


    /**
     * Find a {@code Job} instance by its unique request id
     */
    Job getJob( UUID id )

    /**
     * Save a {@code Job} instance
     *
     * @param job
     * @return
     */
    boolean putJob( Job job )

    Job updateJob( UUID requestId, Closure updateAction )


    /**
     * @return Generate a unique identifier to be used a {@code TaskEntry} key
     */
    TaskId nextTaskId()

    /**
     * Save a {@code TaskEntry} in the underlying storage implementation
     * @param task A {@code TaskEntry} instance
     * @return {@code true} if {@code job} was created as a new entry, {@code false} when the entry is updated
     */
    boolean saveTask( TaskEntry task )

    /**
     * Get a job with the specified ID
     *
     * @param taskId The ID of the requested {@code TaskEntry}
     * @return The {@code TaskEntry} instance associated with specified ID or {@code null} if the job does not exist
     */
    TaskEntry getTask( TaskId taskId )

    /**
     *
     * @param taskId
     * @return
     */
    List<TaskEntry> findTasksById( String taskId );

    /**
     * @return The list of all defined jobs
     */
    List<TaskEntry> findAllTasks()

    /**
     * Find all the tasks currently assigned to a cluster node specified it {@code Address}
     *
     * @param address
     * @return
     */
    List<TaskEntry> findAllTasksOwnerBy( Integer nodeId )

    /**
     * Add a new listener closure invoke when a new {@code TaskEntry} in added in the storage
     *
     * @param listener
     */
    void addNewTaskListener( Closure listener )

    /**
     * Remove the specified instance from the listeners registered
     *
     * @param listener
     */
    void removeNewTaskListener( Closure listener )


    /**
     * Find the jobs in the specified status
     *
     * @param status An open array of job status, it CANNOT be empty
     * @return The list of jobs in status specified, or an empty list when no job matches the status specified
     */
    List<TaskEntry> findTasksByStatus( TaskStatus... status )

    /**
     * Find the jobs in the status by the specified string
     *
     * @param status
     * @return
     */
    List<TaskEntry> findTasksByStatusString( String status )

    List<TaskEntry> findTasksByRequestId( UUID requestId )

    /**
     * The job status statistics i.e. how many jobs for each status
     */
    StatReplyData findTasksStat()

    /**
     * Find the {@code TaskEntry} instance by the specified ID, apply the specified closure and save it
     *
     * @param taskId
     * @param closure
     * @return {@code true} if saved successfully, @{Code false} otherwise
     */
    boolean updateTask( TaskId taskId, Closure closure )

    /**
     * @return The count of {@code TaskEntry} stored
     */
    long countTasks()

    int nextNodeId()

    NodeData getNodeData(int nodeId)

    List<NodeData> findNodeDataByAddress( Address address )

    List<NodeData> findNodeDataByAddressAndStatus( Address address, NodeStatus status )

    NodeData putNodeData( NodeData nodeData )

    boolean replaceNodeData( NodeData oldValue, NodeData newValue )

    NodeData updateNodeData( NodeData current, Closure closure )

    boolean removeNodeData( NodeData dataToRemove )

    List<NodeData> findAllNodesData()

    void shutdown()

}
