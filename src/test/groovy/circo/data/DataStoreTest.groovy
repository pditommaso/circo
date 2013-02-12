package circo.data
import static test.TestHelper.addr

import circo.model.Job
import circo.model.JobStatus
import circo.model.NodeData
import circo.model.TaskEntry
import circo.model.TaskId
import circo.model.TaskReq
import circo.model.TaskStatus
import circo.model.WorkerRefMock
import com.hazelcast.core.Hazelcast
import spock.lang.Specification
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class DataStoreTest extends Specification {


    private void shutdown(def store) {
        if( store instanceof HazelcastDataStore ) {
            Hazelcast.shutdownAll()
        }
    }

    def 'test getJob and putJob' () {

        setup:
        final id = UUID.randomUUID()
        def job = new Job(id)
        job.status = JobStatus.SUBMITTED
        job.missingTasks << TaskId.of(1) << TaskId.of(33)

        when:
        store.putJob(job)

        then:
        store.getJob(id) == job
        store.getJob(UUID.randomUUID()) == null


        cleanup:
        shutdown(store)

        where:
        store << [  new LocalDataStore(), new HazelcastDataStore()  ]

    }

    def 'test updateJob' () {

        setup:
        final id = UUID.randomUUID()
        def job = new Job(id)
        job.status = JobStatus.SUBMITTED
        job.missingTasks << TaskId.of(1) << TaskId.of(33)
        store.putJob(job)

        when:
        def result = store.updateJob( id ) { Job it ->
            it.status = JobStatus.SUBMITTED
        }

        then:
        store.getJob(id).submitted
        store.getJob(id) == result


        cleanup:
        shutdown(store)

        where:
        store << [  new LocalDataStore() ]


    }




    def 'test task get and set '( ) {

        when:
        def id = TaskId.of(1)
        def entry = new TaskEntry( id, new TaskReq(script: 'Hola') )
        def result = store.saveTask(entry)


        then:
        result == true
        entry == store.getTask(id)
        entry == store.getTask(TaskId.of(1))
        null == store.getTask( TaskId.of(321) )

        cleanup:
        shutdown(store)

        where:
        store << [  new LocalDataStore(), new HazelcastDataStore()  ]
    }

    def 'test getTask' () {

        setup:
        def id0 = TaskId.of('123')
        def id1 = TaskId.of('abc')
        def id2 = TaskId.of(222)

        store.saveTask( TaskEntry.create(id1) { it.req.script = 'script1' } )
        store.saveTask( TaskEntry.create(id2) { it.req.script = 'script2' } )

        expect:
        store.getTask(id0) == null
        store.getTask(TaskId.of('abc')).req.script == 'script1'
        store.getTask(TaskId.of(222)).req.script == 'script2'

        cleanup:
        shutdown(store)

        where:
        store << [  new LocalDataStore(), new HazelcastDataStore()  ]

    }

    def testUpdate() {

        when:
        def id = TaskId.of(1)
        def entry = new TaskEntry( id, new TaskReq(script: 'Hola') )
        def resultNew = store.saveTask(entry)

        def resultUpdate = store.updateTask( id ) { TaskEntry it ->
            it.attempts = 2
        }

        then:
        resultNew == true
        resultUpdate == false
        store.getTask(id) .attempts == 2

        cleanup:
        shutdown(store)

        where:
        store << [  new LocalDataStore(), new HazelcastDataStore()  ]

    }

    def 'test add listener' () {

        when:
        TaskEntry invoked = null
        def entry = TaskEntry.create( 1243 )
        store.addNewTaskListener { it ->
            invoked = it
        }
        def firstIsNew = store.saveTask(entry)
        def secondIsNew = store.saveTask(entry)

        then:
        firstIsNew
        !secondIsNew
        invoked == entry

        where:
        store << [ new LocalDataStore(),  new HazelcastDataStore()  ]
    }

    def 'test remove listener' () {

        when:
        def invoked
        def entry = TaskEntry.create( 1243 )
        def count=0
        def callback = { count++ }
        store.addNewTaskListener( callback )
        store.removeNewTaskListener( callback )
        store.saveTask(entry)
        store.saveTask(entry)


        then:
        count == 0

        where:
        store << [  new LocalDataStore(), new HazelcastDataStore()   ]

    }


    def 'test findJobsByStatus' () {

        setup:
        def job1 = TaskEntry.create('1') { it.status = TaskStatus.NEW }
        def job2 = TaskEntry.create('2') { it.status = TaskStatus.PENDING }
        def job3 = TaskEntry.create('3') { it.status = TaskStatus.PENDING }
        def job4 = TaskEntry.create('4') { it.status = TaskStatus.TERMINATED }
        def job5 = TaskEntry.create('5') { it.status = TaskStatus.TERMINATED }
        def job6 = TaskEntry.create('6') { it.status = TaskStatus.TERMINATED }

        store.saveTask(job1)
        store.saveTask(job2)
        store.saveTask(job3)
        store.saveTask(job4)
        store.saveTask(job5)
        store.saveTask(job6)

        expect:
        store.findTasksByStatus(TaskStatus.NEW).toSet() == [job1] as Set
        store.findTasksByStatus(TaskStatus.PENDING).toSet() == [job2,job3] as Set
        store.findTasksByStatus(TaskStatus.TERMINATED).toSet() == [job4,job5,job6] as Set
        store.findTasksByStatus(TaskStatus.READY) == []
        store.findTasksByStatus(TaskStatus.NEW, TaskStatus.PENDING).toSet() == [job1,job2,job3] as Set

        cleanup:
        shutdown(store)

        where:
        store << [ new LocalDataStore(), new HazelcastDataStore() ]

    }

    def 'test findByRequestId' () {
        setup:
        def req1 = UUID.randomUUID()
        def req2 = UUID.randomUUID()

        def job1 = TaskEntry.create('1') { TaskEntry it -> it.status = TaskStatus.NEW; it.req.ticket = req1 }
        def job2 = TaskEntry.create('2') { TaskEntry it -> it.status = TaskStatus.PENDING; it.req.ticket = req1  }
        def job3 = TaskEntry.create('3') { TaskEntry it -> it.status = TaskStatus.PENDING; it.req.ticket = req2  }
        def job4 = TaskEntry.create('4') { TaskEntry it -> it.status = TaskStatus.TERMINATED; it.req.ticket = req2   }
        def job5 = TaskEntry.create('5') { TaskEntry it -> it.status = TaskStatus.TERMINATED; it.req.ticket = req2   }
        def job6 = TaskEntry.create('6') { TaskEntry it -> it.status = TaskStatus.TERMINATED  }

        store.saveTask(job1)
        store.saveTask(job2)
        store.saveTask(job3)
        store.saveTask(job4)
        store.saveTask(job5)
        store.saveTask(job6)

        expect:
        store.findTasksByRequestId( req1 ).toSet() == [job1,job2] as Set
        store.findTasksByRequestId( req2 ).toSet() == [job3,job4,job5] as Set

        cleanup:
        shutdown(store)

        where:
        store << [ new LocalDataStore(), new HazelcastDataStore() ]

    }




    def 'test findJobsByID' () {

        setup:

        def job1 = TaskEntry.create( '11' )
        def job2 = TaskEntry.create( '23' )
        def job3 = TaskEntry.create( '33' )
        def job4 = TaskEntry.create( '34' )
        def job5 = TaskEntry.create( '35' )
        def job6 = TaskEntry.create( '36' )

        store.saveTask(job1)
        store.saveTask(job2)
        store.saveTask(job3)
        store.saveTask(job4)
        store.saveTask(job5)
        store.saveTask(job6)

        expect:
        store.findTasksById( '11' ) == [job1]
        store.findTasksById( '1*' ) == [job1]
        store.findTasksById( '12' ) == []

        store.findTasksById( '33' ) == [job3]
        store.findTasksById( '34' ) == [job4]
        store.findTasksById( '3*' ).toSet() == [job3,job4,job5,job6].toSet()

        store.findTasksById( '*3' ).toSet() == [job2,job3].toSet()

        // preceding '0' are removed
        store.findTasksById( '011' ) == [job1]

        cleanup:
        shutdown(store)

        where:
        store << [ new LocalDataStore(), new HazelcastDataStore() ]

    }


    def 'test findAllTaskAssignedTo' () {

        setup:

        def task1 = TaskEntry.create(1) { TaskEntry it -> it.ownerId = 1 }
        def task2 = TaskEntry.create(2) { TaskEntry it -> it.ownerId = 2 }
        def task3 = TaskEntry.create(3) { TaskEntry it -> it.ownerId = 2 }
        def task4 = TaskEntry.create(4) { TaskEntry it -> it.ownerId = 2 }
        def task5 = TaskEntry.create(5)
        store.saveTask(task1)
        store.saveTask(task2)
        store.saveTask(task3)
        store.saveTask(task4)
        store.saveTask(task5)

        expect:
        store.findAllTasksOwnerBy(1).toSet() == [task1] as Set
        store.findAllTasksOwnerBy(2).toSet() == [task2, task3, task4] as Set
        store.findAllTasksOwnerBy(99) == []

        cleanup:
        shutdown(store)

        where:
        store << [ new LocalDataStore(), new HazelcastDataStore() ]

    }



    def "test getAndPutNodeInfo" () {
        setup:
        def nodeInfo = new NodeData( id: 99, processed: 7843 )
        nodeInfo.createWorkerData( new WorkerRefMock('worker1') )
        nodeInfo.createWorkerData( new WorkerRefMock('worker2') )

        when:
        store.putNodeData(nodeInfo)


        then:
        store.getNodeData(99) == nodeInfo
        store.getNodeData(77) == null

        cleanup:
        shutdown(store)

        where:
        store << [  new LocalDataStore(), new HazelcastDataStore()  ]

    }

    def "test replaceNodeInfo" () {

        setup:
        def node1 = new NodeData( id: 1, processed: 7843 )
        node1.createWorkerData( new WorkerRefMock('worker1') )
        node1.createWorkerData( new WorkerRefMock('worker2') )

        def node2 = new NodeData( id: 2, processed: 343 )

        store.putNodeData(node1)
        store.putNodeData(node2)

        when:
        def copy1 = new NodeData(node1)
        def copy2 = new NodeData(node2)

        def newNode1 = new NodeData( id: 1, processed: 8888 )
        def newNode2 = new NodeData( id: 2, processed: 4444 )

        newNode1.processed++
        copy2.processed++

        then:
        // copy1 is a clone of node1 -- it does not change so, it can be replaced with a new value
        store.replaceNodeData(copy1, newNode1)

        // node2 is changed after it was copied -- the replace will fail
        !store.replaceNodeData(copy2, newNode2)


        cleanup:
        shutdown(store)

        where:
        store << [  new LocalDataStore(), new HazelcastDataStore()  ]
    }

    def "test updateNodeInfo" () {

        setup:
        def node1 = new NodeData( id: 11, processed: 100, failed: 10 )
        node1.createWorkerData( new WorkerRefMock('worker1') )
        node1.createWorkerData( new WorkerRefMock('worker2') )

        def node2 = new NodeData( id:  22, processed: 200, failed:  20 )

        store.putNodeData(node1)
        store.putNodeData(node2)

        // make a copy of this object
        // BUT the 'node2' is updated after the copy
        def copy1 = new NodeData(node1)
        def copy2 = new NodeData(node2)
        node2.processed++
        node2.failed++
        store.putNodeData(node2)

        when:
        def result1 = store.updateNodeData( copy1 ) { NodeData node ->
            node.processed += 10
            node.failed += 10
        }

        def result2 = store.updateNodeData( copy2 ) { NodeData node ->
            node.processed += 10
            node.failed += 10
        }

        then:
        result1.processed  == 110
        result1.failed == 20

        result2.processed == 211   // fail with HZ
        result2.failed == 31


        cleanup:
        shutdown(store)

        where:
        store << [  new LocalDataStore(), new HazelcastDataStore()  ]

    }

    def "test updateNodeInfo for missing entry" () {

        setup:
        def node1 = new NodeData( address: addr('1.1.1.1'), processed: 100, failed: 10 )

        when:
        store.updateNodeData(node1) {  }

        then:
        thrown(IllegalStateException)

        cleanup:
        shutdown(store)

        where:
        store << [  new LocalDataStore(), new HazelcastDataStore()  ]

    }


    def "test updateNodeInfo for workers" () {

        setup:
        def node1 = new NodeData( id: 1, processed: 100, failed: 10 )
        node1.createWorkerData( new WorkerRefMock('/a') )
        node1.createWorkerData( new WorkerRefMock('/b') )
        node1.createWorkerData( new WorkerRefMock('/c') )


        when:
        store.putNodeData(node1)
        node1 = store.updateNodeData(node1) { NodeData it ->

            it.workers.clear()

        }

        then:
        node1.workers.size() == 0
        store.getNodeData(1).workers.size() == 0

        cleanup:
        shutdown(store)

        where:
        store << [  new LocalDataStore(), new HazelcastDataStore()  ]


    }





}
