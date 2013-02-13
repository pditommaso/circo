/*
 * Copyright (c) 2012, the authors.
 *
 *    This file is part of Circo.
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

package circo.model
import static test.TestHelper.addr

import spock.lang.Specification
import test.TestHelper
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class NodeDataTest extends Specification {

    def "test equalsAndHash" () {

        when:
        NodeData node1 = create(11, '/a,/b') { it.address=addr('1.1.1.1'); it.failed=3; it.processed=99 }
        NodeData node2 = create(11, '/a,/b') { it.address=addr('1.1.1.1'); it.failed=3; it.processed=99 }
        NodeData node3 = create(11, '/a,/c') { it.address=addr('1.1.1.2'); it.failed=3; it.processed=98 }
        NodeData node4 = create(11, '/a,/b,/x') { it.address=addr('1.1.1.1'); it.failed=3; it.processed=99 }
        NodeData node5 = create(11, '/a,/b') { it.address=addr('1.1.1.1'); it.failed=3; it.processed=99; it.queue << TaskId.of(5) }

        then:
        node1 == node2
        node1 != node3
        node1 != node4
        node1 != node5

        node1.hashCode() == node2.hashCode()
        node1.hashCode() != node3.hashCode()
        node1.hashCode() != node4.hashCode()
        node1.hashCode() != node5.hashCode()

    }

    def "test copyConstructor" () {

        setup:
        def worker1 = new WorkerRefMock('/w1')
        def worker2 = new WorkerRefMock('/w2')
        def worker3 = new WorkerRefMock('/w3')

        // create a node
        def node1 = new NodeData(id: 99, address: addr('1.1.1.1'), failed: 3, processed: 99 )
        // put something in the queue
        node1.queue << TaskId.of(1) << TaskId.of(2) << TaskId.of(3)

        // add worker
        node1.createWorkerData(worker1)
        node1.createWorkerData(worker2)
        node1.createWorkerData(worker3)

        // assign a task
        node1.assignTaskId(worker1, TaskId.of(4))

        when:
        def node2 = new NodeData( node1 )
        def node3 = new NodeData( node1 )
        node3.failed++

        then:
        node1 == node2
        node1 != node3
        node1.queue.size() == 3
        node1.queue[0] == node2.queue[0]

        node1.numOfBusyWorkers() == 1
        node1.numOfBusyWorkers() == node2.numOfBusyWorkers()

        node1.numOfWorkers() == 3
        node1.numOfWorkers() == node2.numOfWorkers()

        node1.numOfFreeWorkers() == 2
        node1.numOfFreeWorkers() == node2.numOfFreeWorkers()

        node1.numOfQueuedTasks() == 3
        node1.numOfQueuedTasks() == node2.numOfQueuedTasks()
    }


    def "test putAndGetAndHasWorkerData" () {
        setup:
        def worker1 = new WorkerRefMock('/w1')
        def worker2 = new WorkerRefMock('/w2')
        def worker3 = new WorkerRefMock('/w3')

        when:
        def node = new NodeData()
        node.putWorkerData(WorkerData.of(worker1))
        node.putWorkerData(WorkerData.of(worker2))

        then:
        node.hasWorkerData(worker1) == true
        node.hasWorkerData(new WorkerRefMock('/w1')) == true
        node.hasWorkerData(worker2) == true
        node.hasWorkerData(worker3) == false

        node.getWorkerData(worker1) == WorkerData.of(worker1)
        node.getWorkerData(worker2) == WorkerData.of(new WorkerRefMock('/w2'))
        node.getWorkerData(worker3) == null
    }

    def "test failureCounter" () {
        setup:
        def worker1 = new WorkerRefMock('/w1')
        def worker2 = new WorkerRefMock('/w2')
        def worker3 = new WorkerRefMock('/w3')

        when:
        def node = new NodeData()
        node.putWorkerData(WorkerData.of(worker2))
        node.putWorkerData(WorkerData.of(worker3))

        node.failureInc(worker1)

        node.failureInc(worker2)
        node.failureInc(worker2)

        node.failureInc(worker3)
        node.failureInc(worker3)
        node.failureInc(worker3)

        then:
        node.getWorkerData(worker1) == null
        node.getWorkerData(worker2).failed == 2
        node.getWorkerData(worker3).failed == 3
        node.failed == 5

    }

    def "test assignJob" () {

        setup:
        def worker1 = new WorkerRefMock('/w1')
        def worker2 = new WorkerRefMock('/w2')
        def worker3 = new WorkerRefMock('/w3')

        def node = new NodeData()
        node.putWorkerData(WorkerData.of(worker1))
        node.putWorkerData(WorkerData.of(worker2))

        when:
        // assign a job to worker1 -- OK
        def result1 = node.assignTaskId( worker1, TaskId.of(111) )

        // assign a jon to worker2 -- but try to assign another one, return FALSE
        node.assignTaskId( worker2, TaskId.of(222) )
        def result2 = node.assignTaskId( worker2, TaskId.of(888) )  // <-- this is skipped since a job it is already assigned

        // assign a job for a worker that is not in the node -- FAIL
        def result3 = node.assignTaskId( worker3, TaskId.of(333) )

        then:
        result1 == true
        result2 == false
        result3 == false

        node.getWorkerData(worker1).currentTaskId == TaskId.of(111)
        node.getWorkerData(worker2).currentTaskId == TaskId.of(222)
        node.getWorkerData(worker3) == null

        node.getWorkerData(worker1).processed == 1
        node.getWorkerData(worker2).processed == 1
        node.processed == 2



    }

    def "test removeJob" () {

        setup:
        def worker1 = new WorkerRefMock('/w1')
        def worker2 = new WorkerRefMock('/w2')
        def worker3 = new WorkerRefMock('/w3')

        def node = new NodeData()
        node.putWorkerData(WorkerData.of(worker1))
        node.putWorkerData(WorkerData.of(worker2))

        node.assignTaskId( worker1, TaskId.of(111) )
        node.assignTaskId( worker2, TaskId.of(222) )


        when:
        // job 1
        def ret1 = node.removeTaskId(worker1)

        // job 2
        def ret2 = node.removeTaskId( worker2 )
        node.assignTaskId( worker2, TaskId.of(999) )

        // job 3
        def ret3 = node.removeTaskId(worker3)


        then:
        ret1 == TaskId.of(111)
        ret2 == TaskId.of(222)
        ret3 == null

        node.getWorkerData(worker1).currentTaskId == null
        node.getWorkerData(worker2).currentTaskId == TaskId.of(999)
        node.getWorkerData(worker1).processed == 1
        node.getWorkerData(worker2).processed == 2
        node.processed == 3

    }


    def "test eachWorker"() {

        setup:
        def worker1 = new WorkerRefMock('/w1')
        def worker2 = new WorkerRefMock('/w2')
        def worker3 = new WorkerRefMock('/w3')

        def node = new NodeData()
        node.putWorkerData( WorkerData.of(worker1) { it.processed=1 } )
        node.putWorkerData( WorkerData.of(worker2) { it.processed=2 } )
        node.putWorkerData( WorkerData.of(worker3) { it.processed=3 } )

        when:
        def count = 0
        node.eachWorker { WorkerData data -> count += data.processed }

        then:
        count == 6

    }



    /**
     * Helper method to create a {@code NodeData} instance for test purpose
     *
     * @param address The node address e.g. 2.2.2.2:2551
     * @param workers Comma separated list of worker names
     * @param status
     * @return
     */
    static NodeData create( Integer nodeId, String workers, Closure closure = null ) {

        def result = new NodeData(id: nodeId)
        result.address = TestHelper.randomAddress()

        workers?.split('\\,')?.each { it -> result.putWorkerData( WorkerData.of( new WorkerRefMock( it ) )) }

        if ( closure ) closure.call(result)

        return result
    }
}
