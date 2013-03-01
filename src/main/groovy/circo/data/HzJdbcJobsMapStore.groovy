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
import circo.model.Job
import groovy.transform.InheritConstructors
import groovy.util.logging.Slf4j
import org.apache.commons.lang.SerializationUtils

/**
 *
 *  @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */

@Slf4j
@InheritConstructors
class HzJdbcJobsMapStore extends AbstractHzJdbcMapStore<UUID, Job> {


    @Override
    String getTableName() {
        return "JOBS"
    }

    def void createTable( ) {

        sql.execute """
            create table if not exists ${tableName} (
              ID VARCHAR PRIMARY KEY,
              OBJ BINARY
            );
        """.toString()
    }

    @Override
    Object keyToObj(UUID key) {
        return key.toString()
    }

    @Override
    UUID objToKey(def value) {
        return UUID.fromString( value as String )
    }

    List<Job> findByRequestId( String requestId ) {

        if ( requestId.size() < 36 ) {
            // replace wildcards with SQL wildcards
            requestId = requestId.replace("?", "%").replace("*", "%")

            // if not wildcard are provided, append by default
            if ( !requestId.contains('%') ) {
                requestId += '%'
            }
        }


        String statement = "select ID, OBJ from ${tableName} where ID like ?".toString()

        List<Job> result = new LinkedList<>()
        sql.eachRow(statement, requestId) { row ->

            final id = objToKey(row[0])
            Job item = row[1] ? SerializationUtils.deserialize(row[1] as byte[]) as Job : null
            result.add(item)
        }

        return result
    }

}