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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentSkipListSet
import java.util.concurrent.atomic.AtomicInteger

import circo.model.NodeData
import circo.model.TaskEntry
import circo.model.TaskId
import com.google.common.collect.ArrayListMultimap
import com.google.common.collect.Multimap
import groovy.util.logging.Slf4j
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
class LocalDataStore extends AbstractDataStore {

    private AtomicInteger taskIdGen = new AtomicInteger()

    private AtomicInteger nodeIdGen = new AtomicInteger()

    private ConcurrentHashMap<UUID, File> files = new ConcurrentHashMap<>()

    private Multimap<UUID,TaskId> sink = ArrayListMultimap.create()

    def LocalDataStore() {
        jobs = new ConcurrentHashMap<>()
        tasks = new ConcurrentHashMap<>()
        nodes = new ConcurrentHashMap<>()
        killList = new ConcurrentSkipListSet<TaskId>()
    }

    def void shutdown() { }

    def localMemberId() { null }

    def void withTransaction(Closure closure) { closure.call() }


    // ------------------------------ TASKS ------------------------------------------

    /**
     * {@inheritDoc}
     */
    TaskId nextTaskId() {
        new TaskId( taskIdGen.addAndGet(1) )
    }


    // ------------------------------ NODE -------------------------------------------

    /**
     * {@inheritDoc}
     */
    int nextNodeId() {
        nodeIdGen.addAndGet(1)
    }


    NodeData getPartitionNode( def item ) {
        return nodes ? nodes.values().first() : null
    }

    void partitionNodes( List entries, Closure closure ) {
        assert entries
        assert closure

        def theNode = nodes ? nodes.values().first() : null

        entries.each {
            closure.call( it, theNode )
        }
    }

    // ----------------------------- SINK --------------------------------------------

    void addToSink( TaskEntry task ) {
        assert task
        assert task?.req?.requestId

        if( !sink.containsEntry(task.req.requestId, task.id)) {
            sink.put(task.req.requestId, task.id)
        }
    }

    boolean removeFromSink( TaskEntry task ) {
        assert task
        assert task?.req?.requestId

        sink.remove(task.req.requestId, task.id)
    }

    int countTasksMissing( UUID requestId ) {
        sink.get(requestId).size()
    }


    // ----------------------------- FILES -------------------------------------------

    void saveFile( UUID fileId, File file ) {
        files.put(fileId, file)
    }

    File getFile( UUID fileId ) {
        files.get(fileId)
    }




}
