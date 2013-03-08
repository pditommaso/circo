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

import javax.sql.DataSource

import circo.data.sql.JdbcDataSourceFactory
import com.hazelcast.core.MapStore
import groovy.sql.Sql
import groovy.util.logging.Slf4j
import org.apache.commons.lang.SerializationUtils
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */

@Slf4j
abstract class AbstractHzJdbcMapStore<K extends Serializable, V extends Serializable> implements MapStore<K, V> {

    /**
     * @return The underlying SQL table
     */
    abstract String getTableName()

    abstract Object keyToObj( K key )

    abstract K objToKey(def value)

    def V empty(K key) { null }

    /**
     * Store constructor, tries to create the SQL table if not exists
     */
    AbstractHzJdbcMapStore() {
        if ( JdbcDataSourceFactory.instance ) {
            this.sql = new Sql(JdbcDataSourceFactory.instance)
        }
    }

    AbstractHzJdbcMapStore(Sql sql, boolean createTableIfNotExist=false) {
        this.sql = sql

        if ( createTableIfNotExist ) {
            log.debug "Creating DB table: $tableName"
            createTable()
        }
    }

    AbstractHzJdbcMapStore(DataSource dataSource, boolean createTableIfNotExist=false) {
        this(new Sql(dataSource),createTableIfNotExist)
    }


    /**
     * Default table create method
     *
     * @param sql
     */
    def void createTable() {

        sql.execute """
            create table if not exists ${tableName} (
              ID BIGINT PRIMARY KEY,
              OBJ BINARY
            );
        """.toString()
    }


    def void dropTable() {
        sql.execute ("drop table if exists $tableName".toString())
    }


    /**
     * The interval SQL object, this should be used only for test purpose
     */
    private Sql fSql

    protected setSql( Sql sql ) { this.fSql = sql }

    def Sql getSql() {
        if( fSql ) return fSql
        throw new IllegalStateException('Missing JDBC data-source ')
    }


    @Override
    void store(K key, V value) {
        assert key

        // -- serialize and store
        def blob = value ? SerializationUtils.serialize(value) : null
        sql.execute("merge into ${tableName} (ID, OBJ) values (?, ?)".toString(), [ keyToObj(key), blob ])

    }

    @Override
    void storeAll(Map<K, V> map) {
        assert map

        if( map.size() == 0 ) return

        // insert all with a batch operation
        sql.withBatch( map.size(), "merge into ${tableName} (ID, OBJ) values (?,?)".toString() )  { stm ->

            map.each { key, value ->
                def blob = value ? SerializationUtils.serialize(value) : null
                stm.addBatch( keyToObj(key), blob as byte[] )
            }

        }

    }

    @Override
    void delete(K key) {
        assert key
        sql.execute("delete from ${tableName} where ID = ?".toString(), [keyToObj(key)])
    }

    @Override
    void deleteAll(Collection<K> keys) {
        assert keys
        if ( !keys ) return

        def params = ['?'] * keys.size()
        String statement = "delete from ${tableName} where ID in (${params.join(',')})".toString()
        List keysToDelete = keys.collect { keyToObj(it) }

        sql.execute(statement, keysToDelete as List<Object>)
    }

    @Override
    V load(K key) {
        assert key

        def row = sql.firstRow("select OBJ from ${tableName} where ID = ?".toString(), [keyToObj(key)])
        if( !row ) {
            return null
        }

        row[0] ? SerializationUtils.deserialize( row[0] as byte[] ) as V : empty(key)
    }

    @Override
    Map<K, V> loadAll(Collection<K> keys) {

        def result = new HashMap<K,V>( keys.size() )
        if( !keys ) { return result }

        def params = ['?'] * keys.size()
        String statement = "select ID, OBJ from ${tableName} where ID in (${params.join(',')})".toString()
        List values = keys.collect { keyToObj(it) }

        sql.eachRow(statement, values) { row ->

            final id = objToKey(row[0])
            final item = row[1] ? SerializationUtils.deserialize(row[1] as byte[]) : null
            result.put( id, item as V )
        }

        return result
    }

    /**
     * Disable entries pre-loading by returning always {@code null}
     *
     * return {@code null}
     */
    @Override
    Set<K> loadAllKeys() { null  }



    List<V> loadAll() {

        def result = new LinkedList<V>()
        def statement = "select OBJ from ${tableName} ".toString()

        sql.eachRow(statement) { row ->

            final item = row[0] ? SerializationUtils.deserialize(row[0] as byte[]) : null
            result.add( item as V)
        }

        return result

    }



}
