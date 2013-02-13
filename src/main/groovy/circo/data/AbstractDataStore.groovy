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
import java.util.concurrent.locks.Lock

import akka.actor.Address
import circo.model.Job
import circo.model.NodeData
import circo.model.NodeStatus
import circo.model.TaskEntry
import circo.model.TaskId
import circo.model.TaskStatus
import circo.reply.StatReplyData
import groovy.util.logging.Slf4j
import org.apache.commons.lang.SerializationUtils

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
abstract class AbstractDataStore implements DataStore {


    protected ConcurrentMap<UUID,Job> jobs

    protected ConcurrentMap<TaskId, TaskEntry> tasks

    protected ConcurrentMap<Integer, NodeData> nodeData

    protected abstract Lock getLock( def key )

    final protected void withLock(TaskId taskId, Closure closure) {
        def lock = getLock(taskId)
        lock.lock()
        try {
            closure.delegate = this
            closure.call()
        }
        finally {
            lock.unlock()
        }
    }

    Job getJob( UUID id ) {
        jobs.get(id)
    }

    boolean putJob( Job job ) {
        assert job
        assert job.requestId

        jobs.put(job.requestId, job) != null
    }

    Job updateJob( UUID requestId, Closure updateMethod ) {
        assert requestId
        update(requestId, jobs, updateMethod)
    }


    List<Job> findAllJobs() {
        new ArrayList<Job>(jobs.values())
    }

    boolean saveTask( TaskEntry task ) {
        assert task
        def old = tasks.put( task.id, task )
        log.trace "## save ${task.dump()} -- was ${old?.dump()}"

        return old == null
    }

    @Override
    TaskEntry getTask( TaskId taskId) {
        assert taskId
        tasks.get(taskId)
    }

    boolean updateTask( TaskId taskId, Closure closure ) {
        def entry = getTask(taskId)
        closure.call(entry)
        saveTask( entry )
    }

    long countTasks() { tasks.size() }


    StatReplyData findTasksStat() {
        def result = new StatReplyData()

        result.pending = findTasksByStatus( TaskStatus.PENDING ).size()
        result.running = findTasksByStatus( TaskStatus.RUNNING ).size()

        def terminated = findTasksByStatus( TaskStatus.TERMINATED )
        result.successful = terminated.count { TaskEntry it -> it.success }.toInteger()
        result.failed = terminated.count { TaskEntry it -> it.failed }.toInteger()

        return result
    }


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
     * TODO +++ refactor using query api
     *
     * @param requestId
     * @return
     */
    List<TaskEntry> findTasksByRequestId( UUID requestId ) {

        def result = tasks.values().findAll { TaskEntry task ->
            task?.req?.ticket == requestId
        }

        return new ArrayList<>(result)
    }


    List<TaskEntry> findAllTasks() {
        new ArrayList<>(tasks.values())
    }

    NodeData getNodeData( int nodeId ) {
        nodeData.get(nodeId)
    }

    @Override
    List<NodeData> findNodeDataByAddress( Address address ) {
        assert address

        List<NodeData> result = []
        nodeData.values().each { NodeData node ->
            if ( node.address == address ) {
                result << node
            }
        }

        return result
    }

    @Override
    List<NodeData> findNodeDataByAddressAndStatus( Address address, NodeStatus status ) {

        List<NodeData> result = []
        nodeData.values().each { NodeData node ->
            if ( node.address == address && (!status || node.status == status)  ) {
                result << node
            }
        }

        return result
    }

    @Override
    NodeData putNodeData( NodeData nodeData) {
        assert nodeData
        this.nodeData.put(nodeData.id, nodeData)
    }

    boolean replaceNodeData( NodeData oldValue, NodeData newValue ) {
        assert oldValue
        assert newValue
        assert oldValue.id == newValue.id

        nodeData.replace(oldValue.id, oldValue, newValue)
    }

    NodeData updateNodeData( NodeData node, Closure closure ) {
        assert node

        def begin = System.currentTimeMillis()
        def done = false
        while( !done ) {
            // make a copy of the data structure and invoke the closure in it
            def copy = new NodeData(node)
            closure.call(copy)

            // try to replace it in the map
            done = replaceNodeData(node, copy)
            if( done ) {
                // -- OK, return the 'copy'
                node = copy
            }
            // -- the update operation failed
            //    try it again reloading the 'NodeInfo' from the storage
            else if( System.currentTimeMillis()-begin < 1000 ) {
                node = getNodeData(copy.id)
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


    def boolean removeNodeData( NodeData dataToRemove ) {
        assert dataToRemove
        nodeData.remove(dataToRemove.address, dataToRemove)
    }


    def List<NodeData> findAllNodesData() {
        new ArrayList<NodeData>(nodeData.values())
    }

    /**
     * Implements a generic 'optmistic' concurrent update for the object with the ID specified
     * <p>
     *     Given the object ID, this method load the associated object in the map specified, after this
     *     the closure is invoked passing the loaded object as parameter.
     *     <p>
     *     The closure may update the object fields and when if exit, the method replace the old
     *     object with the new one using the {@code ConcurrentMap#replace(K,V,V)} method
     *     <p>
     *     The replace is retried until it is able to be fulfilled
     *
     * @param id The object ID in the map
     * @param map The map container
     * @param closure The update action
     * @return The updated object stored in the map
     */
    protected <T extends Serializable> T update( def id, ConcurrentMap<?,T> map, Closure<T> closure ) {

        int count=0
        def begin = System.currentTimeMillis()
        def done = false
        T newValue
        while( !done ) {
            // make a copy of the data structure and invoke the closure in it
            T value = map.get(id)
            newValue = SerializationUtils.clone(value) as T
            closure.call(newValue)

            // try to replace it in the map
            if( value == newValue ) {
                return value
            }

            done = map.replace(id, value, newValue)
            if( !done ) {
                if ( System.currentTimeMillis()-begin > 10_000 ) {
                    throw new TimeoutException("** Update failed (${++count}), unable to replace: ${value.dump()} -- with: ${newValue.dump()} ")
                }
                else {
                    log.debug "Update failed (${++count}), can't replace: ${value} -- with: ${newValue}"
                    sleep 50
                }
            }

        }

        return newValue
    }

}
