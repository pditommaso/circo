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

import com.hazelcast.core.MapStore
import groovy.sql.Sql
import groovy.util.logging.Slf4j
import org.apache.commons.lang.SerializationUtils
import circo.model.TaskEntry
import circo.model.TaskId

import javax.sql.DataSource
/**
 *
 *  @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */

@Slf4j
class JdbcJobsMapStore implements MapStore<TaskId,TaskEntry> {


    def static createTable( def Sql sql ) {
        assert sql

        sql.execute """
            create table if not exists JOBS (
              ID BIGINT PRIMARY KEY,
              OBJ BINARY
            );
        """

    }

    /**
     * The JDBC datasource -- it is defined static since it will be inject during the application
     * bootstrap with a single value
     */
    private static DataSource fDataSource

    def static void setDataSource( DataSource dataSource ) {
        assert dataSource

        // assign the data source
        fDataSource = dataSource

        // create the tables if not exists
        createTable( new Sql(dataSource) )

    }

    /**
     * The interval SQL object, this should be used only for test purpose
     */
    private Sql fSql

    protected setSql( Sql sql ) { this.fSql = sql }

    def Sql getSql() {
        if( fSql ) return fSql
        if ( fDataSource ) return new Sql(fDataSource)
        throw new IllegalStateException('Missing JDBC datasource property ')
    }

    @Override
    void store(TaskId key, TaskEntry value) {
        assert key

        // -- delete it eventually
        delete(key)

        // -- serialize and store
        def blob = value ? SerializationUtils.serialize(value) : null
        sql.execute("insert into JOBS (ID, OBJ) values (?, ?)", [ key.value, blob ])

    }

    @Override
    void storeAll(Map<TaskId, TaskEntry> map) {
        assert map

        if( map.size() == 0 ) return

        // -- delete all
        deleteAll(map.keySet())

        // insert all with a batch operation
        sql.withBatch( map.size(), "insert into JOBS (id, obj) values (?,?)" )  { stm ->

            map.each { key, value ->
                def blob = value ? SerializationUtils.serialize(value) : null
                stm.addBatch( key.value, blob as byte[] )
            }

        }

    }

    @Override
    void delete(TaskId key) {
        assert key
        sql.execute("delete from JOBS where id = ?", [key.value])
    }

    @Override
    void deleteAll(Collection<TaskId> keys) {
        assert keys
        if ( !keys ) return

        def params = ['?'] * keys.size()
        String statement = "delete from JOBS where ID in (${params.join(',')})"
        List keysToDelete = keys.collect { it.value }

        sql.execute(statement, keysToDelete as List<Object>)
    }

    @Override
    TaskEntry load(TaskId key) {
        assert key

        def row = sql.firstRow("select OBJ from JOBS where ID = ?", [key.value])
        if( !row ) {
            return null
        }

        row[0] ? SerializationUtils.deserialize( row[0] as byte[] ) as TaskEntry : TaskEntry.create(key)
    }

    @Override
    Map<TaskId, TaskEntry> loadAll(Collection<TaskId> keys) {

        def result = new HashMap<TaskId,TaskEntry>( keys.size() )
        if( !keys ) { return result }

        def params = ['?'] * keys.size()
        String statement = "select ID, OBJ from JOBS where ID in (${params.join(',')})"
        List values = keys.collect { it.value }

        sql.eachRow(statement, values) { row ->

            final id = TaskId.of( row[0] )
            final task = row[1] ? SerializationUtils.deserialize(row[1] as byte[]) : null
            result.put( id, task as TaskEntry)
        }

        return result
    }

    @Override
    Set<TaskId> loadAllKeys() {

        def result = new LinkedHashSet<TaskId>()
        sql.eachRow("select ID from JOBS ") { row ->
            result << TaskId.of( row[0] )
        }

        return result
    }
}