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
import circo.model.TaskId
import groovy.transform.InheritConstructors

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */

@InheritConstructors
class HzJdbcSinkMapStore extends AbstractHzJdbcMapStore<TaskId,UUID> {

    @Override
    String getTableName() { "SINK" }

    @Override
    def void createTable() {
        assert sql

        sql.execute """
            create table if not exists ${tableName} (
              ID BIGINT PRIMARY KEY,
              REQUEST_ID VARCHAR
            );

            create index if not exists ndx_sink_1 on $tableName (REQUEST_ID);

        """.toString()
    }


    @Override
    Object keyToObj(TaskId key) { key?.value }

    @Override
    TaskId objToKey(value) { return TaskId.of(value) }


    @Override
    void store(TaskId key, UUID value) {
        assert key

        // -- serialize and store
        sql.execute("merge into ${tableName} (ID, REQUEST_ID) values (?, ?)".toString(), [ keyToObj(key), value?.toString() ])

    }

    @Override
    void storeAll(Map<TaskId, UUID> map) {
        assert map

        if( map.size() == 0 ) return

        // insert all with a batch operation
        sql.withBatch( map.size(), "merge into ${tableName} (ID, REQUEST_ID) values (?,?)".toString() )  { stm ->

            map.each { key, value ->
                stm.addBatch( keyToObj(key), value?.toString() )
            }

        }
    }

    @Override
    UUID load(TaskId key) {
        assert key

        def row = sql.firstRow("select REQUEST_ID from ${tableName} where ID = ?".toString(), [keyToObj(key)])
        if( !row ) {
            return null
        }

        row[0] ? UUID.fromString(row[0] as String): null
    }


    @Override
    Map<TaskId, UUID> loadAll(Collection<TaskId> keys) {

        def result = new HashMap<TaskId,UUID>( keys.size() )
        if( !keys ) { return result }

        def params = ['?'] * keys.size()
        String statement = "select ID, REQUEST_ID from ${tableName} where ID in (${params.join(',')})".toString()
        List values = keys.collect { keyToObj(it) }

        sql.eachRow(statement, values) { row ->

            final id = objToKey(row[0])
            final item = row[1] ? UUID.fromString(row[1]) : null
            result.put( id, item )
        }

        return result
    }


    /**
     * @param requestId The request UUID for which the number of tasks is requested
     * @return The number of tasks found
     */
    int countByRequestId( UUID requestId ) {

        def statement = "select count(*) from ${tableName} where REQUEST_ID = ?".toString()
        def result = sql.firstRow(statement, requestId.toString())

        result ? result[0] as Integer : 0
    }
}
