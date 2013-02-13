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
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock

import circo.model.TaskEntry
import circo.model.TaskId
import circo.model.TaskStatus
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@CompileStatic
class LocalDataStore extends AbstractDataStore {

    private List jobsListeners = []

    private AtomicInteger idGen = new AtomicInteger()

    private AtomicInteger nodeIdGen = new AtomicInteger()

    def LocalDataStore() {
        jobs = new ConcurrentHashMap<>()
        tasks = new ConcurrentHashMap<>()
        nodeData = new ConcurrentHashMap<>()
    }

    def void shutdown() { }

    TaskId nextTaskId() { new TaskId( idGen.addAndGet(1) ) }

    int nextNodeId() { nodeIdGen.addAndGet(1) }

    @Override
    protected Lock getLock(key) {
        new ReentrantLock()
    }


    List<TaskEntry> findTasksByStatus( TaskStatus... status ) {
        assert status
        tasks.values().findAll { TaskEntry task -> task.status in status  }
    }

    boolean saveTask( TaskEntry task) {
        def isNew = super.saveTask(task)

        if( isNew && jobsListeners )  {
            try {
                jobsListeners.each{ Closure it -> it.call(task) }
            }
            catch( Exception e ) {
                log.error "Failed invoking Add New TaskEntry listener", e
            }

        }

        return isNew
    }

    List<TaskEntry> findTasksById( final String taskId) {
        assert taskId

        def value
        if ( taskId.contains('*') ) {
            value = taskId.replace('*','.*')
        }
        else {
            value = taskId
        }

        // remove '0' prefix
        while( value.size()>1 && value.startsWith('0') ) { value = value.substring(1) }

        tasks.values().findAll { TaskEntry task -> task.id.toFmtString() ==~ /$value/ }

    }

    @Override
    List<TaskEntry> findAllTasksOwnerBy(Integer nodeId) {
        assert nodeId

        return tasks.values().findAll() { TaskEntry it -> it.ownerId == nodeId }
    }

    /**
     * Find all jobs with the status specified
     *
     * @param status
     * @return
     */
    @Override
    void addNewTaskListener(Closure listener) {
        assert listener
        jobsListeners.add(listener)
    }


    void removeNewTaskListener(Closure listener) {
        assert listener
        jobsListeners.remove(listener)
    }

}
