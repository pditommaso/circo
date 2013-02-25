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

import circo.model.TaskEntry
import circo.model.TaskId
import circo.model.TaskStatus
import groovy.sql.Sql
import org.apache.commons.lang.SerializationUtils

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class HzJdbcTasksMapStore extends AbstractHzJdbcMapStore<TaskId, TaskEntry> {


    @Override
    String getTableName() { "TASKS" }

    @Override
    def void createTable( def Sql sql ) {
        assert sql

        sql.execute """
            create table if not exists ${tableName} (
              ID BIGINT PRIMARY KEY,
              OBJ BINARY,
              OWNER_ID INT,
              STATUS VARCHAR,
              REQUEST_ID VARCHAR
            );

            create index if not exists ndx_tasks_1 on $tableName (OWNER_ID);
            create index if not exists ndx_tasks_2 on $tableName (STATUS);
            create index if not exists ndx_tasks_3 on $tableName (REQUEST_ID);

        """.toString()
    }

    @Override
    Object keyToObj(TaskId key) { key.value }

    @Override
    TaskId objToKey(value) { TaskId.of(value) }

    @Override
    def TaskEntry empty(TaskId key) { TaskEntry.create(key) }

    @Override
    void store(TaskId key, TaskEntry value) {
        assert key
        assert value

        // -- serialize and store
        def blob = SerializationUtils.serialize(value)
        sql.execute("merge into ${tableName} (ID, OBJ, OWNER_ID, STATUS, REQUEST_ID) values (?, ?, ?, ?, ?)".toString(), [
                keyToObj(key),
                blob,
                value.ownerId,
                value.status?.toString(),
                value?.req?.ticket?.toString()
        ])

    }

    @Override
    void storeAll(Map<TaskId, TaskEntry> map) {
        assert map

        if( map.size() == 0 ) return

        // insert all with a batch operation
        sql.withBatch( map.size(), "merge into ${tableName} (ID, OBJ, OWNER_ID, STATUS, REQUEST_ID) values (?,?,?,?,?)".toString() )  { stm ->

            map.each { key, value ->
                def blob = value ? SerializationUtils.serialize(value) : null
                Integer ownerId = value?.ownerId
                String status = value?.status?.toString()
                String requestId = value?.req?.ticket?.toString()
                stm.addBatch( keyToObj(key), blob as byte[], ownerId, status, requestId )
            }

        }

    }



    // --------------------------------- FINDERS -------------------------------------------------


    List<TaskEntry> findByStatus( TaskStatus... status ) {
        assert status

        def result = new LinkedList<TaskEntry>()

        def marks = ['?'] * status.size()
        def values = status *. toString()
        def statement = "select OBJ from ${tableName} where STATUS in ( ${marks.join(',')} )".toString()
        sql.eachRow(statement, values) { row ->
            final task = row[0] ? SerializationUtils.deserialize(row[0] as byte[]) : null
            result.add( task as TaskEntry )
        }

        return result
    }

    List<TaskEntry> findByOwnerId( int ownerId ) {

        def result = new LinkedList<TaskEntry>()
        def statement = "select OBJ from ${tableName} where OWNER_ID = ?".toString()
        sql.eachRow(statement, [ownerId]) { row ->
            final task = row[0] ? SerializationUtils.deserialize(row[0] as byte[]) : null
            result.add( task as TaskEntry )
        }

        return result
    }


    List<TaskEntry> findByRequestId( UUID requestId ) {
        assert requestId

        def result = new LinkedList<TaskEntry>()
        def statement = "select OBJ from ${tableName} where REQUEST_ID = ?".toString()
        sql.eachRow(statement, [requestId.toString()]) { row ->
            final task = row[0] ? SerializationUtils.deserialize(row[0] as byte[]) : null
            result.add( task as TaskEntry )
        }

        return result

    }



}
