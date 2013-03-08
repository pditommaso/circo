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
    void saveJob( Job job )

    boolean updateJob( UUID requestId, Closure updateAction )

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
    void saveTask( TaskEntry task )

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
    void saveNode( NodeData nodeData )

    /**
     *
     * @param nodeId
     * @param update
     * @return
     */
    boolean updateNode( int nodeId, Closure update )

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

    /**
     * Given an item return the partition node where it should be allocated
     *
     * @param item A generic item
     * @return The {@code NodeData} where the item should be allocated
     */
    NodeData getPartitionNode( def item )

    /**
     * Partition a list of entries mapping each to a {@code NodeData} instance
     *
     * @param entries A generic list of entries
     * @param closure A {@code Closure} invoked for each entry mapping the value to the corresponding {@code NodeData} - For example { Object item, NodeData node -> ... }
     */
    void partitionNodes( List entries, Closure closure )


    // ----------------------------- FILES operations -------------------------------------

    File getFile( UUID fileId )

    void saveFile( UUID fileId, File file )


    // ------------------------------- other methods --------------------------------------

    /**
     * Execute the provide code block by wrapping into a transaction
     * @param closure
     */
    void withTransaction(Closure closure)

    /**
     * Shut-down the underlying data store
     */
    void shutdown()

    /**
     * The storage member unique id to identify the node in the cluster.
     * See {@code NodeData#storeMemberId}
     *
     * @return Currently defined b the pair ( IP, port ) address of the in-memory storage
     */
    def localMemberId()


}
