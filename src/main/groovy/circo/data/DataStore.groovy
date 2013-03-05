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
import circo.model.JobStatus
import circo.model.NodeData
import circo.model.NodeStatus
import circo.model.TaskEntry
import circo.model.TaskId
import circo.model.TaskStatus
/**
 * Define the operations provided by the data storage system
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
interface DataStore {

    /**
     * Shut-down the underlying data store
     */
    void shutdown()

    void withTransaction(Closure closure)


    // ------------- tasks queue --------------------------------

    void appendToQueue( TaskId taskId )

    TaskId takeFromQueue()

    boolean isEmptyQueue()


    // -------------- JOB operations ----------------------------


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
    void storeJob( Job job )

    boolean updateJob( UUID requestId, Closure updateAction )

    boolean replaceJob( Job oldValue, Job newValue )

    List<Job> findJobsByRequestId( String requestId )

    List<Job> findJobsByStatus( JobStatus... status )

    List<Job> listJobs()



    // ----------------- tasks operations -----------------------

    /**
     * @return Generate a unique identifier to be used a {@code TaskEntry} key
     */
    TaskId nextTaskId()

    /**
     * Save a {@code TaskEntry} in the underlying storage implementation
     * @param task A {@code TaskEntry} instance
     * @return {@code true} if {@code job} was created as a new entry, {@code false} when the entry is updated
     */
    void storeTask( TaskEntry task )

    /**
     * Get a job with the specified ID
     *
     * @param taskId The ID of the requested {@code TaskEntry}
     * @return The {@code TaskEntry} instance associated with specified ID or {@code null} if the job does not exist
     */
    TaskEntry getTask( TaskId taskId )


    boolean updateTask( TaskId taskId, Closure callback )

    /**
     * @return The list of all defined jobs
     */
    List<TaskEntry> listTasks()

    /**
     * Find all the tasks currently assigned to a cluster node specified it {@code Address}
     *
     * @param address
     * @return
     */
    List<TaskEntry> findTasksByOwnerId( Integer nodeId )


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

    /**
     * Find all {@code TaskEntry} created by the request specified
     *
     * @param requestId
     * @return The list of tasks or an empty list if not task exist for the specified ID
     */
    List<TaskEntry> findTasksByRequestId( UUID requestId )

    List<TaskEntry> findTasksByRequestId( String requestId )

    // ----------------------------- TASKS support operations ------------------------

    void addToSink( TaskEntry task )

    boolean removeFromSink( TaskEntry task )

    int countTasksMissing( UUID requestId )

    void addToKillList( TaskId task )

    boolean removeFromKillList( TaskId task )


    // --------------- NODE operations -------------------------------------

    /**
     * @return A unique identifier for a cluster node instance
     */
    int nextNodeId()

    /**
     * Get a node data structure by its unique ID
     *
     * @param nodeId The node primary key
     * @return The associated {@code NodeData} instance of {@code null} if don't exist
     */
    NodeData getNode(int nodeId)

    /**
     * Store the specified {@code NodeData} object into the storage
     * @param nodeData The object to be saved
     */
    void storeNode( NodeData nodeData )

    /**
     * Replace an existing {code NodeData} instance - oldValue - by an updated version
     * @param oldValue
     * @param newValue
     * @return
     */
    boolean replaceNode( NodeData oldValue, NodeData newValue )

    /**
     * Remove the specified {@code NodeData}
     *
     * @param nodeToRemove The {@code NodeData} instance to be removed
     * @return {@code true} if removed successfully or {@code false} otherwise
     */
    boolean removeNode( NodeData nodeToRemove )

    /**
     * @return All the {@code NodeData} instances or an empty list if no data is available
     */
    List<NodeData> listNodes()


    /**
     * Find all the node associated with the specified address
     *
     * @param address The node {@code Address} of the required {@code NodeData}
     * @return The list of matching nodes or an empty list if there no nodes for the specified address
     */
    List<NodeData> findNodesByAddress( Address address )

    /**
     * Find all the nodes for the specified address and status
     * @param address The node {@code Address} of the required {@code NodeData}
     * @param status The {@code NodeStatus} of the required {@code NodeData}
     * @return The list of matching nodes or an empty list if there no nodes for the specified address
     */
    List<NodeData> findNodesByAddressAndStatus( Address address, NodeStatus status )


    // ----------------------------- FILES operations -------------------------------------

    File getFile( UUID fileId )

    void storeFile( UUID fileId, File file )


}
