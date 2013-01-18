/*
 * Copyright (c) 2012, the authors.
 *
 *    This file is part of RUSH.
 *
 *    Moke is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation, either version 3 of the License, or
 *    (at your option) any later version.
 *
 *    Moke is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with RUSH.  If not, see <http://www.gnu.org/licenses/>.
 */

package rush.data
import groovy.sql.Sql
import rush.messages.JobEntry
import rush.messages.JobId
import spock.lang.Specification
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class JdbcJobsMapStoreTest extends Specification {

    def Sql sql

    /**
     * Set up a H2 in-memory database
     * <p>
     *     H2 SQL reference
     *     http://www.h2database.com/html/grammar.html
     *
     */
    def setup() {

        sql = Sql.newInstance('jdbc:h2:mem:Rush')
        sql.execute ("drop table if exists JOBS")

        JdbcJobsMapStore.createTable(sql)

    }


    def 'test store' () {

        setup:
        def store = new JdbcJobsMapStore(sql: this.sql)
        def job1 = new JobEntry( '1122', 'Script1' )
        def job2 = new JobEntry( '4455', 'Script2' )

        when:
        store.store(job1.id, job1)
        store.store(job2.id, job2)

        then:
        store.loadAllKeys().size() != 0
        store.loadAllKeys().size() == 2
        store.loadAllKeys() == [ job1.id, job2.id ] as Set
        store.load( job1.id ) ==  new JobEntry( '1122', 'Script1' )
        store.load( job1.id ).req.script == 'Script1'
        store.load( job1.id ) !=  new JobEntry( '333', 'Script2' )
    }

    def 'test StoreAll ' () {

        setup:
        def store = new JdbcJobsMapStore(sql: this.sql)
        def job1 = new JobEntry( '1122', 'Script1' )
        def job2 = new JobEntry( '4455', 'Script2' )
        def job3 = new JobEntry( new JobId('4455',99), 'Script3' )
        def job4 = new JobEntry( new JobId('7788',44), 'Script4' )

        def map = new HashMap<JobId,JobEntry>()
        map[job1.id] = job1
        map[job2.id] = job2
        map[job3.id] = job3
        map[job4.id] = job4


        when:
        store.storeAll( map )
        println store.loadAllKeys()

        then:
        store.loadAllKeys().size() == 4
        store.load( job1.id ).id == JobId.fromString('1122')
        store.load( job1.id ).req.script == 'Script1'

        store.load( job4.id ).id == JobId.fromString('7788:44')
        store.load( job4.id ).req.script == 'Script4'
    }


    def 'test delete ' () {
        setup:
        def store = new JdbcJobsMapStore(sql: this.sql)
        def job1 = new JobEntry( '1122', 'Script1' )
        def job2 = new JobEntry( '4455', 'Script2' )
        def job3 = new JobEntry( new JobId('4455',99), 'Script3' )
        def job4 = new JobEntry( new JobId('7788',44), 'Script4' )

        def map = new HashMap<JobId,JobEntry>()
        map[job1.id] = job1
        map[job2.id] = job2
        map[job3.id] = job3
        map[job4.id] = job4
        store.store( job1.id, job1 )
        store.store( job2.id, job2 )
        store.store( job3.id, job3 )
        store.store( job4.id, job4 )

        when:
        store.delete( job3.id )

        then:
        store.load( job3.id )  == null
        store.load( job1.id ) == job1
        store.loadAllKeys().size() == 3

    }


    def 'test deleteAll' () {
        setup:
        def store = new JdbcJobsMapStore(sql: this.sql)
        def job1 = new JobEntry( '1122', 'Script1' )
        def job2 = new JobEntry( '4455', 'Script2' )
        def job3 = new JobEntry( new JobId('4455',99), 'Script3' )
        def job4 = new JobEntry( new JobId('7788',44), 'Script4' )

        store.store( job1.id, job1 )
        store.store( job2.id, job2 )
        store.store( job3.id, job3 )
        store.store( job4.id, job4 )

        when:
        store.deleteAll( [job1.id, job3.id, job4.id ] )

        then:
        store.load( job3.id )  == null
        store.load( job2.id ) == job2
        store.loadAllKeys().size() == 1
    }

    def 'test load' () {
        setup:
        def store = new JdbcJobsMapStore(sql: this.sql)
        def job1 = new JobEntry( '1122', 'Script1' )
        def job2 = new JobEntry( '4455', 'Script2' )
        def job3 = new JobEntry( new JobId('4455',99), 'Script3' )
        def job4 = new JobEntry( new JobId('7788',44), 'Script4' )


        when:
        store.store( job1.id, job1 )
        store.store( job2.id, job2 )
        store.store( job3.id, job3 )
        store.store( job4.id, job4 )

        then:
        store.load( job1.id )  == job1
        store.load( job2.id ) != null
        store.load( job2.id ) != job1
        store.load( JobId.fromString('777') )  == null
    }

    def 'test allKeys ' () {
        setup:
        def store = new JdbcJobsMapStore(sql: this.sql)
        def job1 = new JobEntry( '1122', 'Script1' )
        def job2 = new JobEntry( '4455', 'Script2' )
        def job3 = new JobEntry( new JobId('4455',99), 'Script3' )
        def job4 = new JobEntry( new JobId('7788',44), 'Script4' )

        def map = new HashMap<JobId,JobEntry>()
        map[job1.id] = job1
        map[job2.id] = job2
        map[job3.id] = job3
        map[job4.id] = job4

        when:
        store.storeAll( map )

        then:
        store.loadAllKeys() == map.keySet()

    }

    def 'test loadAll' () {
        setup:
        def store = new JdbcJobsMapStore(sql: this.sql)
        def job1 = new JobEntry( '1122', 'Script1' )
        def job2 = new JobEntry( '4455', 'Script2' )
        def job3 = new JobEntry( new JobId('4455',99), 'Script3' )
        def job4 = new JobEntry( new JobId('7788',44), 'Script4' )


        when:
        store.store( job1.id, job1 )
        store.store( job2.id, job2 )
        store.store( job3.id, job3 )
        store.store( job4.id, job4 )


        then:
        store.loadAll( [job1.id, job2.id, job4.id] ) == [ (job1.id):job1, (job2.id):job2, (job4.id):job4 ]

    }


}
