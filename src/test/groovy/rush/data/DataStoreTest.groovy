package rush.data

import com.hazelcast.core.Hazelcast
import rush.messages.JobEntry
import rush.messages.JobId
import rush.messages.JobReq
import rush.messages.JobStatus
import spock.lang.Specification

import static test.TestHelper.addr
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
    
    def testSaveAndLoad( ) {

        when:
        def id = JobId.of(1)
        def entry = new JobEntry( id, new JobReq(script: 'Hola') )
        def result = store.saveJob(entry)


        then:
        result == true
        entry == store.getJob(id)
        entry == store.getJob(JobId.of(1))
        null == store.getJob( JobId.of(321) )

        cleanup:
        shutdown(store)

        where:
        store << [  new LocalDataStore(), new HazelcastDataStore()  ]

    }

    def testUpdate() {

        when:
        def id = JobId.of(1)
        def entry = new JobEntry( id, new JobReq(script: 'Hola') )
        def resultNew = store.saveJob(entry)

        def resultUpdate = store.updateJob( id ) { JobEntry it ->
            it.attempts = 2
        }

        then:
        resultNew == true
        resultUpdate == false
        store.getJob(id) .attempts == 2

        cleanup:
        shutdown(store)

        where:
        store << [  new LocalDataStore(), new HazelcastDataStore()  ]

    }

    def 'test add listener' () {
        when:
        def invoked
        def entry = JobEntry.create( 1243 )
        def count=0
        store.addNewJobListener { invoked = it; count++ }
        store.saveJob(entry)
        store.saveJob(entry)


        then:
        count == 1
        invoked == entry

        where:
        store << [  new LocalDataStore(), new HazelcastDataStore()   ]
    }

    def 'test remove listener' () {

        when:
        def invoked
        def entry = JobEntry.create( 1243 )
        def count=0
        def callback = { count++ }
        store.addNewJobListener( callback )
        store.removeNewJobListener( callback )
        store.saveJob(entry)
        store.saveJob(entry)


        then:
        count == 0

        where:
        store << [  new LocalDataStore(), new HazelcastDataStore()   ]

    }


    def 'test findJobsByStatus' () {

        setup:
        def job1 = JobEntry.create('1') { it.status = JobStatus.NEW }
        def job2 = JobEntry.create('2') { it.status = JobStatus.PENDING }
        def job3 = JobEntry.create('3') { it.status = JobStatus.PENDING }
        def job4 = JobEntry.create('4') { it.status = JobStatus.COMPLETE }
        def job5 = JobEntry.create('5') { it.status = JobStatus.COMPLETE }
        def job6 = JobEntry.create('6') { it.status = JobStatus.COMPLETE }

        store.saveJob(job1)
        store.saveJob(job2)
        store.saveJob(job3)
        store.saveJob(job4)
        store.saveJob(job5)
        store.saveJob(job6)

        expect:
        store.findJobsByStatus(JobStatus.NEW).toSet() == [job1] as Set
        store.findJobsByStatus(JobStatus.PENDING).toSet() == [job2,job3] as Set
        store.findJobsByStatus(JobStatus.COMPLETE).toSet() == [job4,job5,job6] as Set
        store.findJobsByStatus(JobStatus.READY) == []
        store.findJobsByStatus(JobStatus.NEW, JobStatus.PENDING).toSet() == [job1,job2,job3] as Set

        cleanup:
        shutdown(store)

        where:
        store << [ new LocalDataStore(), new HazelcastDataStore() ]

    }

    def 'test findJobsByID' () {

        setup:
        def id1 = UUID.randomUUID()
        def id2 = UUID.randomUUID()
        def id3 = UUID.randomUUID()

        def job1 = JobEntry.create( id1 )
        def job2 = JobEntry.create( id2 )

        def job3 = JobEntry.create( new JobId(id3,1) )
        def job4 = JobEntry.create( new JobId(id3,2) )
        def job5 = JobEntry.create( new JobId(id3,3) )
        def job6 = JobEntry.create( new JobId(id3,11) )

        store.saveJob(job1)
        store.saveJob(job2)
        store.saveJob(job3)
        store.saveJob(job4)
        store.saveJob(job5)
        store.saveJob(job6)

        expect:
        store.findJobsById( id1.toString() ) == [job1]
        store.findJobsById( id1.toString().substring(0,8) ) == [job1]
        store.findJobsById( id1.toString().substring(0,8)+'xx' ) == []

        store.findJobsById( new JobId(id3,1).toString() ) == [job3]
        store.findJobsById( new JobId(id3,2).toString() ) == [job4]
        store.findJobsById( id3.toString() ).toSet() == [job3,job4,job5,job6].toSet()

        store.findJobsById( new JobId(id3,1).toString()+'*' ).toSet() == [job3,job6].toSet()


        cleanup:
        shutdown(store)

        where:
        store << [ new LocalDataStore(), new HazelcastDataStore() ]

    }



    def "test getAndPutNodeInfo" () {
        setup:
        def nodeInfo = new NodeData( address: addr('1.2.2.2'), processed: 7843 )
        nodeInfo.createWorkerData( new WorkerRefMock('worker1') )
        nodeInfo.createWorkerData( new WorkerRefMock('worker2') )

        when:
        store.putNodeData(nodeInfo)


        then:
        store.getNodeData(addr('1.2.2.2')) == nodeInfo
        store.getNodeData(addr('4.6.6.6')) == null

        cleanup:
        shutdown(store)

        where:
        store << [  new LocalDataStore(), new HazelcastDataStore()  ]

    }

    def "test replaceNodeInfo" () {

        setup:
        def node1 = new NodeData( address: addr('1.1.1.1'), processed: 7843 )
        node1.createWorkerData( new WorkerRefMock('worker1') )
        node1.createWorkerData( new WorkerRefMock('worker2') )

        def node2 = new NodeData( address: addr('2.2.2.2'), processed: 343 )

        def node3 = new NodeData( address:  addr('3.3.3.3') )

        def newNode1 = new NodeData( address: addr('1.1.1.1'), processed: 8888 )
        def newNode2 = new NodeData( address: addr('2.2.2.2'), processed: 4444 )
        def newNode3 = new NodeData( address:  addr('3.3.3.3'))

        store.putNodeData(node1)
        store.putNodeData(node2)
        store.putNodeData(node3)


        when:
        def copy1 = new NodeData(node1)
        def copy2 = new NodeData(node2)
        def copy3 = new NodeData(node3)

        newNode1.processed++
        node2.processed++
        copy3.processed++

        then:
        // copy1 is a clone of node1 -- it does not change so, it can be replaced with a new value
        store.replaceNodeData(copy1, newNode1) == true

        // node2 is changed after it was copied -- the replace will fail
        store.replaceNodeData(copy2, newNode2) == false

        store.replaceNodeData(copy3, newNode3) == false

        cleanup:
        shutdown(store)

        where:
        store << [  new LocalDataStore(), new HazelcastDataStore()  ]
    }

    def "test updateNodeInfo" () {

        setup:
        def node1 = new NodeData( address: addr('1.1.1.1'), processed: 100, failed: 10 )
        node1.createWorkerData( new WorkerRefMock('worker1') )
        node1.createWorkerData( new WorkerRefMock('worker2') )

        def node2 = new NodeData( address: addr('2.2.2.2'), processed: 200, failed:  20 )

        store.putNodeData(node1)
        store.putNodeData(node2)

        // make a copy of this object
        // BUT the 'node2' is updated after the copy
        def copy1 = new NodeData(node1)
        def copy2 = new NodeData(node2)
        node2.processed++
        node2.failed++

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

        result2.processed == 211
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
        def node1 = new NodeData( address: addr('1.1.1.1'), processed: 100, failed: 10 )
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
        store.getNodeData(addr('1.1.1.1')).workers.size() == 0

        cleanup:
        shutdown(store)

        where:
        store << [  new LocalDataStore(), new HazelcastDataStore()  ]


    }





}
