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
import groovy.sql.Sql
import groovy.util.logging.Slf4j

/**
 *
 *  @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */

@Slf4j
class HzJdbcJobsMapStore extends AbstractHzJdbcMapStore<UUID, Job> {


    @Override
    String getTableName() {
        return "JOBS"
    }

    def void createTable( def Sql sql ) {
        assert sql

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

}