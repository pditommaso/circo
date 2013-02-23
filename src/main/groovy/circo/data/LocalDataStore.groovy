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

import circo.model.TaskId
import groovy.util.logging.Slf4j
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
class LocalDataStore extends AbstractDataStore {

    private AtomicInteger idGen = new AtomicInteger()

    private AtomicInteger nodeIdGen = new AtomicInteger()

    def LocalDataStore() {
        jobs = new ConcurrentHashMap<>()
        tasks = new ConcurrentHashMap<>()
        nodes = new ConcurrentHashMap<>()
        queue = new ConcurrentHashMap<>()
        files = new ConcurrentHashMap<>()
    }

    def void shutdown() { }

    TaskId nextTaskId() { new TaskId( idGen.addAndGet(1) ) }

    int nextNodeId() { nodeIdGen.addAndGet(1) }


}
