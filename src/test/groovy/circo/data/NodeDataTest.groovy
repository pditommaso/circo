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

package circo.data

import akka.actor.Address
import circo.messages.JobId
import spock.lang.Specification

import static test.TestHelper.addr

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class NodeDataTest extends Specification {

    def "test equalsAndHash" () {

        when:
        def node1 = new NodeData(address: addr('1.1.1.1'), failed: 3, processed: 99 )
        def node2 = new NodeData(address: addr('1.1.1.1'), failed: 3, processed: 99 )
        def node3 = new NodeData(address: addr('1.1.1.2'), failed: 3, processed: 98 )

        node1.createWorkerData(new WorkerRefMock('/a'))
        node1.createWorkerData(new WorkerRefMock('/b'))

        node2.createWorkerData(new WorkerRefMock('/a'))
        node2.createWorkerData(new WorkerRefMock('/b'))

        then:
        node1 == node2
        node1 != node3

        node1.hashCode() == node2.hashCode()
        node1.hashCode() != node3.hashCode()

    }

    def "test copyConstructor" () {

        when:
        def node1 = new NodeData(address: addr('1.1.1.1'), failed: 3, processed: 99 )
        def node2 = new NodeData( node1 )
        def node3 = new NodeData( node1 )
        node3.failed++

        then:
        node1 == node2
        node1 != node3
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
        def result1 = node.assignJobId( worker1, JobId.of(111) )

        // assign a jon to worker2 -- but try to assign another one, return FALSE
        node.assignJobId( worker2, JobId.of(222) )
        def result2 = node.assignJobId( worker2, JobId.of(888) )  // <-- this is skipped since a job it is already assigned

        // assign a job for a worker that is not in the node -- FAIL
        def result3 = node.assignJobId( worker3, JobId.of(333) )

        then:
        result1 == true
        result2 == false
        result3 == false

        node.getWorkerData(worker1).currentJobId == JobId.of(111)
        node.getWorkerData(worker2).currentJobId == JobId.of(222)
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

        node.assignJobId( worker1, JobId.of(111) )
        node.assignJobId( worker2, JobId.of(222) )


        when:
        // job 1
        def ret1 = node.removeJobId(worker1)

        // job 2
        def ret2 = node.removeJobId( worker2 )
        node.assignJobId( worker2, JobId.of(999) )

        // job 3
        def ret3 = node.removeJobId(worker3)


        then:
        ret1 == JobId.of(111)
        ret2 == JobId.of(222)
        ret3 == null

        node.getWorkerData(worker1).currentJobId == null
        node.getWorkerData(worker2).currentJobId == JobId.of(999)
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


    def "test toStringFmt"() {

        setup:
        def worker1 = new WorkerRefMock('/w1')
        def worker2 = new WorkerRefMock('/w2')
        def worker3 = new WorkerRefMock('/w3')

        def node = new NodeData()
        node.address = new Address('akka','def','192.44.12.1', 2551)
        node.status = NodeStatus.AVAIL
        node.putWorkerData( WorkerData.of(worker1) { it.processed=1 } )
        node.putWorkerData( WorkerData.of(worker2) { it.processed=2 } )
        node.putWorkerData( WorkerData.of(worker3) { it.processed=3 } )

        when:
        def count = 0
        node.eachWorker { WorkerData data -> count += data.processed }

        println node.toFmtString()

        then:
        node.toFmtString()

    }

    /**
     * Helper method to create a {@code NodeData} instance for test purpose
     *
     * @param address The node address e.g. 2.2.2.2:2551
     * @param workers Comma separated list of worker names
     * @param status
     * @return
     */
    static NodeData create( String address, String workers, NodeStatus status = NodeStatus.AVAIL ) {

        def addrSplices = address.split('\\:')
        def node = new NodeData()
        node.address = new Address('akka','def', addrSplices[0] , addrSplices.size()>1 ? addrSplices[1].toInteger() : 2551 )
        node.status = status

        workers?.split('\\,')?.each { IT -> node.putWorkerData( WorkerData.of( new WorkerRefMock( IT ) )) }

        return node
    }
}
