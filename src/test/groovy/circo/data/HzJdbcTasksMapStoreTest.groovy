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
import circo.model.TaskReq
import circo.model.TaskStatus
import groovy.sql.Sql
import spock.lang.Shared
import spock.lang.Specification
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class HzJdbcTasksMapStoreTest extends Specification {

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
        // the in mem database
        sql = Sql.newInstance('jdbc:h2:mem:Circo')

        def store = new HzJdbcTasksMapStore(sql:sql)
        store.dropTable()
        store.createTable()
    }


    def 'test store' () {

        setup:
        def store = new HzJdbcTasksMapStore(sql: this.sql)
        def job1 = new TaskEntry( '1122', 'Script1' )
        def job2 = new TaskEntry( '4455', 'Script2' )

        when:
        store.store(job1.id, job1)
        store.store(job2.id, job2)

        then:
        store.loadAllKeys().size() != 0
        store.loadAllKeys().size() == 2
        store.loadAllKeys() == [ job1.id, job2.id ] as Set
        store.load( job1.id ) ==  new TaskEntry( '1122', 'Script1' )
        store.load( job1.id ).req.script == 'Script1'
        store.load( job1.id ) !=  new TaskEntry( '333', 'Script2' )
    }

    def 'test StoreAll ' () {

        setup:
        def store = new HzJdbcTasksMapStore(sql: this.sql)
        def job1 = new TaskEntry( '1122', 'Script1' )
        def job2 = new TaskEntry( '4455', 'Script2' )
        def job3 = new TaskEntry( new TaskId('4456'), 'Script3' )
        def job4 = new TaskEntry( new TaskId('7788'), 'Script4' )

        def map = new HashMap<TaskId,TaskEntry>()
        map[job1.id] = job1
        map[job2.id] = job2
        map[job3.id] = job3
        map[job4.id] = job4


        when:
        store.storeAll( map )
        println store.loadAllKeys()

        then:
        store.loadAllKeys().size() == 4
        store.load( job1.id ).id == TaskId.of('1122')
        store.load( job1.id ).req.script == 'Script1'

        store.load( job4.id ).id == TaskId.of('7788')
        store.load( job4.id ).req.script == 'Script4'
    }


    def 'test delete ' () {
        setup:
        def store = new HzJdbcTasksMapStore(sql: this.sql)
        def job1 = new TaskEntry( '1122', 'Script1' )
        def job2 = new TaskEntry( '4455', 'Script2' )
        def job3 = new TaskEntry( new TaskId('4456'), 'Script3' )
        def job4 = new TaskEntry( new TaskId('7788'), 'Script4' )

        def map = new HashMap<TaskId,TaskEntry>()
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
        def store = new HzJdbcTasksMapStore(sql: this.sql)
        def job1 = new TaskEntry( '1122', 'Script1' )
        def job2 = new TaskEntry( '4455', 'Script2' )
        def job3 = new TaskEntry( new TaskId('4456'), 'Script3' )
        def job4 = new TaskEntry( new TaskId('7788'), 'Script4' )

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
        def store = new HzJdbcTasksMapStore(sql: this.sql)
        def job1 = new TaskEntry( '1122', 'Script1' )
        def job2 = new TaskEntry( '4455', 'Script2' )
        def job3 = new TaskEntry( new TaskId('4456'), 'Script3' )
        def job4 = new TaskEntry( new TaskId('7788'), 'Script4' )


        when:
        store.store( job1.id, job1 )
        store.store( job2.id, job2 )
        store.store( job3.id, job3 )
        store.store( job4.id, job4 )

        then:
        store.load( job1.id )  == job1
        store.load( job2.id ) != null
        store.load( job2.id ) != job1
        store.load( TaskId.fromString('777') )  == null
    }

    def 'test allKeys ' () {
        setup:
        def store = new HzJdbcTasksMapStore(sql: this.sql)
        def job1 = new TaskEntry( '1122', 'Script1' )
        def job2 = new TaskEntry( '4455', 'Script2' )
        def job3 = new TaskEntry( new TaskId('4455'), 'Script3' )
        def job4 = new TaskEntry( new TaskId('7788'), 'Script4' )

        def map = new HashMap<TaskId,TaskEntry>()
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
        def store = new HzJdbcTasksMapStore(sql: this.sql)
        def job1 = new TaskEntry( 111, 'Script1' )
        def job2 = new TaskEntry( 222, 'Script2' )
        def job3 = new TaskEntry( new TaskId(333), 'Script3' )
        def job4 = new TaskEntry( new TaskId(444), 'Script4' )


        when:
        store.store( job1.id, job1 )
        store.store( job2.id, job2 )
        store.store( job3.id, job3 )
        store.store( job4.id, job4 )


        then:
        store.loadAll( [job1.id, job2.id, job4.id] ) == [ (job1.id):job1, (job2.id):job2, (job4.id):job4 ]
        store.loadAll() as Set == [ job1, job2, job3, job4 ] as Set

    }


    def 'test findByStatus' () {

        setup:
        def store = new HzJdbcTasksMapStore(sql: this.sql)
        def job1 = TaskEntry.create(111) { TaskEntry it -> it.status = TaskStatus.NEW }
        def job2 = TaskEntry.create(333) { TaskEntry it -> it.status = TaskStatus.PENDING }
        def job3 = TaskEntry.create(444) { TaskEntry it -> it.status = TaskStatus.PENDING }
        def job4 = TaskEntry.create(555) { TaskEntry it -> it.status = TaskStatus.TERMINATED }
        def job5 = TaskEntry.create(666) { TaskEntry it -> it.status = TaskStatus.TERMINATED }
        def job6 = TaskEntry.create(777) { TaskEntry it -> it.status = TaskStatus.TERMINATED }

        when:
        store.store( job1.id, job1 )
        store.store( job2.id, job2 )
        store.store( job3.id, job3 )
        store.store( job4.id, job4 )
        store.store( job5.id, job5 )
        store.store( job6.id, job6 )

        then:
        store.findByStatus(TaskStatus.NEW) == [ job1 ]
        store.findByStatus(TaskStatus.PENDING) as Set == [ job2, job3 ] as Set
        store.findByStatus(TaskStatus.TERMINATED) as Set == [ job4,job5,job6 ] as Set
        store.findByStatus(TaskStatus.NEW, TaskStatus.PENDING) as Set == [ job1, job2, job3 ] as Set
        store.findByStatus(TaskStatus.READY) == []

    }


    def 'test findByOwnerId' () {

        setup:
        def store = new HzJdbcTasksMapStore(sql: this.sql)
        def job1 = TaskEntry.create(111) { TaskEntry it -> it.ownerId = 1 }
        def job2 = TaskEntry.create(222) { TaskEntry it -> it.ownerId = 2 }
        def job3 = TaskEntry.create(333) { TaskEntry it -> it.ownerId = 2 }
        def job4 = TaskEntry.create(444) { TaskEntry it -> it.ownerId = 3 }
        def job5 = TaskEntry.create(555) { TaskEntry it -> it.ownerId = 3 }
        def job6 = TaskEntry.create(666) { TaskEntry it -> it.ownerId = 3 }

        when:
        store.store( job1.id, job1 )
        store.store( job2.id, job2 )
        store.store( job3.id, job3 )
        store.store( job4.id, job4 )
        store.store( job5.id, job5 )
        store.store( job6.id, job6 )

        then:
        store.findByOwnerId(1) == [ job1 ]
        store.findByOwnerId(2) as Set == [ job2, job3 ] as Set
        store.findByOwnerId(3) as Set == [ job4,job5,job6 ] as Set
        store.findByOwnerId(4) == []

    }


    def 'test findByRequestId' () {

        setup:
        def store = new HzJdbcTasksMapStore(sql: this.sql)
        def req1 = UUID.randomUUID()
        def req2 = UUID.randomUUID()
        def req3 = UUID.randomUUID()
        def req4 = UUID.randomUUID()

        def job1 = new TaskEntry( new TaskId(111), new TaskReq(requestId: req1))
        def job2 = new TaskEntry( new TaskId(222), new TaskReq(requestId: req2))
        def job3 = new TaskEntry( new TaskId(333), new TaskReq(requestId: req2))
        def job4 = new TaskEntry( new TaskId(444), new TaskReq(requestId: req3))
        def job5 = new TaskEntry( new TaskId(555), new TaskReq(requestId: req3))
        def job6 = new TaskEntry( new TaskId(666), new TaskReq(requestId: req3))

        when:
        store.store( job1.id, job1 )
        store.store( job2.id, job2 )
        store.store( job3.id, job3 )
        store.store( job4.id, job4 )
        store.store( job5.id, job5 )
        store.store( job6.id, job6 )

        then:
        store.findByRequestId(req1) == [ job1 ]
        store.findByRequestId(req2) as Set == [ job2, job3 ] as Set
        store.findByRequestId(req3) as Set == [ job4,job5,job6 ] as Set
        store.findByRequestId(req4) == []

    }


}
