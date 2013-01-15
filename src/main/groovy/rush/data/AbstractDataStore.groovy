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
import groovy.util.logging.Slf4j
import rush.messages.JobEntry
import rush.messages.JobId

import java.util.concurrent.ConcurrentMap
import java.util.concurrent.TimeoutException
import java.util.concurrent.locks.Lock
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@CompileStatic
abstract class AbstractDataStore implements DataStore {

    protected ConcurrentMap<JobId, JobEntry> jobsMap

    protected ConcurrentMap<Address, NodeData> nodeDataMap

    protected abstract Lock getLock( def key )

    final protected void withLock(JobId jobId, Closure closure) {
        def lock = getLock(jobId)
        lock.lock()
        try {
            closure.delegate = this
            closure.call()
        }
        finally {
            lock.unlock()
        }
    }
    
    boolean saveJob( JobEntry job ) {
        assert job

        jobsMap.put( job.id, job ) == null
    }

    @Override
    JobEntry getJob( JobId id ) {
        assert id
        jobsMap.get(id)
    }

    boolean updateJob( JobId jobId, Closure closure ) {
        def entry = getJob(jobId)
        closure.call(entry)
        saveJob( entry )
    }

    long countJobs() { jobsMap.size() }

    List<JobEntry> findAll() {
        new LinkedList<>(jobsMap.values())
    }


    @Override
    NodeData getNodeData( Address address ) {
        nodeDataMap.get(address)
    }

    @Override
    NodeData putNodeData( NodeData nodeData) {
        assert nodeData
        nodeDataMap.put(nodeData.address, nodeData)
    }

    boolean replaceNodeData( NodeData oldValue, NodeData newValue ) {
        assert oldValue
        assert newValue
        assert oldValue.address == newValue.address

        nodeDataMap.replace(oldValue.address, oldValue, newValue)
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
                node = getNodeData(copy.address)
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
        nodeDataMap.remove(dataToRemove.address, dataToRemove)
    }


    def List<NodeData> findAllNodesData() {
        new ArrayList<NodeData>(nodeDataMap.values())
    }


}
