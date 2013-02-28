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
import java.util.concurrent.atomic.AtomicInteger

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
        queue = new ConcurrentHashMap<>()
    }

    def void shutdown() { }

    def void withTransaction(Closure closure) { closure.call() }

    // ------------------------------ TASKS ------------------------------------------

    TaskId nextTaskId() { new TaskId( taskIdGen.addAndGet(1) ) }

    // ------------------------------ NODE -------------------------------------------

    int nextNodeId() { nodeIdGen.addAndGet(1) }

    // ----------------------------- SINK --------------------------------------------

    void storeTaskSink( TaskEntry task ) {
        assert task
        assert task?.req?.ticket

        sink.put(task.req.ticket,task.id)
    }

    boolean removeTaskSink( TaskEntry task ) {
        assert task
        assert task?.req?.ticket

        sink.remove(task.req.ticket, task.id)
    }

    int countTasksMissing( UUID requestId ) {
        sink.get(requestId).size()
    }


    // ----------------------------- FILES -------------------------------------------

    void storeFile( UUID fileId, File file ) {
        files.put(fileId, file)
    }

    File getFile( UUID fileId ) {
        files.get(fileId)
    }


}
