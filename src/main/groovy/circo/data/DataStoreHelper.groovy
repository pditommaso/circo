/*
 * Copyright (c) 2012, the authors.
 *
 *    This file is part of 'Circo'.
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

import circo.model.TaskEntry
import circo.model.TaskId
import groovy.util.logging.Slf4j
import org.apache.commons.lang.SerializationUtils
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
class DataStoreHelper {

    static private rnd = new Random()

    static TaskId takeFromQueue(ConcurrentMap<TaskId,Boolean> queue) {

        def start = System.currentTimeMillis()

        while( true ) {
            TaskId taskId = queue.keySet().iterator().next()
            if ( !taskId ) return null

            if ( queue.remove(taskId, Boolean.TRUE) ) {
                return taskId
            }

            if( System.currentTimeMillis()-start > 10_000 ) {
                throw new TimeoutException('Unable to take a TaskId from tasks queue')
            }
        }

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
    public static <T extends Serializable> boolean update( def id, ConcurrentMap<?, T> map, Closure<T> closure ) {

        int count=0
        def begin = System.currentTimeMillis()
        def done = false
        T newValue
        while( !done ) {
            // make a copy of the data structure and invoke the closure in it
            T value = map.get(id)
            if ( !value ) {
                return false
            }
            newValue = SerializationUtils.clone(value) as T
            closure.call(newValue)

            // if nothing has changed, return false == nothing has changed
            if( value.equals( newValue ) ) {
                return false
            }

            // try to update it
            done = map.replace(id, value, newValue)
            if( !done ) {
                if ( System.currentTimeMillis()-begin > 10_000 ) {
                    throw new TimeoutException("** Update failed (${++count}), unable to replace: ${value.dump()} -- with: ${newValue.dump()} ")
                }
                else {
                    log.debug "Update failed (${++count}), can't replace: ${value} -- with: ${newValue}"
                    // a 'random' sleep between 5 and 50 ms
                    sleep 5 * (rnd.nextInt(10)+1)
                }
            }

        }

        return done
    }


    @Deprecated
    static public List<TaskEntry> findTasksById( Collection<TaskEntry> collection, final String taskId ) {

        def value
        if ( taskId.contains('*') ) {
            value = taskId.replace('*','.*')
        }
        else {
            value = taskId
        }

        collection.findAll { TaskEntry task -> task.id.toString() ==~ /$value/ }

    }


}
