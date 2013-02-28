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

import java.util.concurrent.ConcurrentMap
import java.util.concurrent.TimeoutException

import akka.actor.Address
import circo.model.Job
import circo.model.NodeData
import circo.model.NodeStatus
import circo.model.TaskEntry
import circo.model.TaskId
import circo.model.TaskStatus
import groovy.util.logging.Slf4j
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
abstract class AbstractDataStore implements DataStore {


    protected ConcurrentMap<UUID,Job> jobs

    protected ConcurrentMap<TaskId, TaskEntry> tasks

    protected ConcurrentMap<Integer, NodeData> nodes

    @Deprecated
    protected ConcurrentMap<TaskId, Boolean> queue



    // ------------- Task QUEUE (?) --------------------------------

    TaskId takeFromQueue() {

        DataStoreHelper.takeFromQueue( queue )

    }

    void appendToQueue( TaskId taskId )  {
        queue.put( taskId, Boolean.TRUE )
    }

    boolean isEmptyQueue() {
        queue.isEmpty()
    }

    // -------------- JOB operations ----------------------------

    Job getJob( UUID id ) {
        jobs.get(id)
    }

    void storeJob( Job job ) {
        assert job
        assert job.requestId

        jobs.put(job.requestId, job)
    }

    boolean updateJob( UUID requestId, Closure updateMethod ) {
        assert requestId
        DataStoreHelper.update(requestId, jobs, updateMethod)
    }

    boolean replaceJob( Job oldValue, Job newValue ) {
        assert oldValue
        assert newValue
        assert oldValue.requestId == newValue.requestId

        jobs.replace(oldValue.requestId, oldValue, newValue)
    }

    List<Job> listJobs() {
        jobs.values().toList()
    }


    // ------------------------- TASKS operation -----------------------------------------

    @Override
    void storeTask( TaskEntry task ) {
        assert task
        tasks.put( task.id, task )
    }

    @Override
    TaskEntry getTask( TaskId taskId) {
        assert taskId
        tasks.get(taskId)
    }


    @Override
    List<TaskEntry> findTasksByStatus( TaskStatus... status ) {
        assert status

        def result = new LinkedList<TaskEntry>()
        listTasks().each{ TaskEntry task ->
            if ( task.status in status ) result << task
        }

        return result
    }


    @Override
    List<TaskEntry> findTasksByOwnerId(Integer nodeId) {
        assert nodeId

        def result = new LinkedList<>()

        listTasks().each { TaskEntry it ->
            if ( it.ownerId == nodeId ) result << it
        }

        return result
    }


    @Override
    List<TaskEntry> findTasksByStatusString( String status ) {

        if( status?.toLowerCase() in ['s','success'] ) {
            return findTasksByStatus(TaskStatus.TERMINATED).findAll {  TaskEntry it -> it.success }
        }

        if ( status?.toLowerCase() in ['e','error','f','failed'] ) {
            return findTasksByStatus(TaskStatus.TERMINATED).findAll {  TaskEntry it -> it.failed }
        }

        findTasksByStatus(TaskStatus.fromString(status))

    }

    /**
     * @param requestId
     * @return
     */
    @Override
    List<TaskEntry> findTasksByRequestId( UUID requestId ) {

        def result = new LinkedList<TaskEntry>()

        listTasks().each { TaskEntry task ->
            if ( task?.req?.ticket == requestId ) result << task
        }

        return result
    }


    @Override
    List<TaskEntry> listTasks() {
        tasks.values().toList()
    }

    // ------------------------------- SINK operations --------------------------------------


    void storeTaskSink( TaskEntry task ) {
        assert task
        assert task?.req?.ticket

        sink.put( task.id, task.req.ticket )
    }

    boolean removeTaskSink( TaskEntry task ) {
        assert task
        assert task?.req?.ticket

        sink.remove(task.id, task.req.ticket )
    }




    // ------------------------------- NODE DATA operations ---------------------------------

    @Override
    NodeData getNode( int nodeId ) {
        nodes.get(nodeId)
    }

    @Override
    List<NodeData> findNodesByAddress( Address address ) {
        assert address

        List<NodeData> result = []
        listNodes().each { NodeData node ->
            if ( node.address == address ) {
                result << node
            }
        }

        return result
    }

    @Override
    List<NodeData> findNodesByAddressAndStatus( Address address, NodeStatus status ) {

        List<NodeData> result = []
        findNodesByAddress(address).each { NodeData node ->
            if ( !status || node.status == status  ) {
                result << node
            }
        }

        return result
    }

    @Override
    void storeNode( NodeData nodeData) {
        assert nodeData
        nodes.put(nodeData.id, nodeData)
    }

    @Override
    boolean replaceNode( NodeData oldValue, NodeData newValue ) {
        assert oldValue
        assert newValue
        assert oldValue.id == newValue.id

        nodes.replace(oldValue.id, oldValue, newValue)
    }

    @Deprecated
    NodeData updateNodeData( NodeData node, Closure closure ) {
        assert node

        def begin = System.currentTimeMillis()
        def done = false
        while( !done ) {
            // make a copy of the data structure and invoke the closure in it
            def copy = new NodeData(node)
            closure.call(copy)

            // try to replace it in the map
            done = replaceNode(node, copy)
            if( done ) {
                // -- OK, return the 'copy'
                node = copy
            }
            // -- the update operation failed
            //    try it again reloading the 'NodeInfo' from the storage
            else if( System.currentTimeMillis()-begin < 1000 ) {
                node = getNode(copy.id)
                if ( !node ) {
                    throw new IllegalStateException("Cannot update NodeData item because not entry exists for address ${copy.address}")
                }
            }
            // timeout exceed, throw an exception
            else {
                throw new TimeoutException("Unable to apply updates to ${node}")
            }

        }

        return node
    }


    @Override
    def boolean removeNode( NodeData dataToRemove ) {
        assert dataToRemove
        nodes.remove(dataToRemove.id, dataToRemove)
    }


    @Override
    def List<NodeData> listNodes() {
        nodes.values().toList()
    }


}
