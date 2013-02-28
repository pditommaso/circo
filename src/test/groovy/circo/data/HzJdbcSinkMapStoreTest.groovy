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
import groovy.sql.Sql
import spock.lang.Shared
import spock.lang.Specification
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class HzJdbcSinkMapStoreTest extends Specification {

    @Shared
    def Sql sql

    /**
     * Set up a H2 in-memory database
     * <p>
     *     H2 SQL reference
     *     http://www.h2database.com/html/grammar.html
     *
     */
    def setup() {

        sql = Sql.newInstance('jdbc:h2:mem:Circo')

        def store = new HzJdbcSinkMapStore(sql: sql)
        store.dropTable()
        store.createTable()
    }


    def 'test count' () {
        setup:
        def jdbc = new HzJdbcSinkMapStore(sql: sql)

        def req1 = UUID.randomUUID()
        def req2 = UUID.randomUUID()

        jdbc.store( TaskId.of(1), req1 )
        jdbc.store( TaskId.of(2), req1 )
        jdbc.store( TaskId.of(3), req2 )
        jdbc.store( TaskId.of(4), req2 )
        jdbc.store( TaskId.of(5), req2 )

        expect:
        jdbc.countByRequestId(req1) == 2
        jdbc.countByRequestId(req2) == 3
        jdbc.countByRequestId(UUID.randomUUID()) == 0


    }

    def 'test load and store' () {
        setup:
        def jdbc = new HzJdbcSinkMapStore(sql: sql)

        def req1 = UUID.randomUUID()
        def req2 = UUID.randomUUID()

        when:
        jdbc.store( TaskId.of(1), req1 )
        jdbc.store( TaskId.of(2), req1 )
        jdbc.store( TaskId.of(3), req2 )
        jdbc.store( TaskId.of(4), req2 )
        jdbc.store( TaskId.of(5), req2 )

        then:
        jdbc.load(TaskId.of(1)) == req1
        jdbc.load(TaskId.of(2)) == req1
        jdbc.load(TaskId.of(3)) == req2
        jdbc.load(TaskId.of(4)) == req2
        jdbc.load(TaskId.of(9)) == null
    }


    def 'test loadAll and storeAll' () {
        setup:
        def jdbc = new HzJdbcSinkMapStore(sql: sql)

        def req1 = UUID.randomUUID()
        def req2 = UUID.randomUUID()

        def map = [
                (TaskId.of(1)):req1,
                (TaskId.of(2)):req1,
                (TaskId.of(3)):req2,
                (TaskId.of(4)):req2,
                (TaskId.of(5)):req2
        ]

        when:
        jdbc.storeAll( map )

        then:
        jdbc.loadAll( [TaskId.of(1), TaskId.of(2), TaskId.of(3), TaskId.of(4), TaskId.of(5)] ) == map
    }


    def 'test loadAllKeys' () {
        setup:
        def jdbc = new HzJdbcSinkMapStore(sql: sql)

        def req1 = UUID.randomUUID()
        def req2 = UUID.randomUUID()

        jdbc.store( TaskId.of(1), req1 )
        jdbc.store( TaskId.of(2), req1 )
        jdbc.store( TaskId.of(3), req2 )

        expect:
        jdbc.loadAllKeys().toSet() == [TaskId.of(1), TaskId.of(2), TaskId.of(3) ] as Set
    }


}
