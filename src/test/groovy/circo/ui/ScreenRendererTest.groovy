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

package circo.ui
import akka.testkit.JavaTestKit
import circo.model.NodeData
import circo.model.TaskEntry
import circo.model.TaskId
import circo.model.TaskReq
import test.ActorSpecification
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class ScreenRendererTest extends ActorSpecification {


    def "test RenderBock" () {
        
        setup:

        def node1 = new NodeData(id: 11)
        def w1 = node1.createWorkerData( new JavaTestKit(system).getRef() )
        def w2 = node1.createWorkerData( new JavaTestKit(system).getRef() )
        def w3 = node1.createWorkerData( new JavaTestKit(system).getRef() )
        def w4 = node1.createWorkerData( new JavaTestKit(system).getRef() )
        node1.processed = 10
        node1.failed = 1
        dataStore.storeNode(node1)

        def node2 = new NodeData(id: 22)
        def w5 = node2.createWorkerData( new JavaTestKit(system).getRef() )
        def w6 = node2.createWorkerData( new JavaTestKit(system).getRef() )
        dataStore.storeNode(node2)

        dataStore.storeTask( new TaskEntry(TaskId.of(1), new TaskReq(script: 'do this')) )
        dataStore.storeTask( new TaskEntry(TaskId.of(2), new TaskReq(script: 'do that')) )
        dataStore.storeTask( new TaskEntry(TaskId.of(3), new TaskReq(script: 'do more')) )


        when:
        def block = new ScreenRenderer(11, dataStore)
        def result = block.render()
        print result

        then:
        result.size()>0

    }

    def "test compare" () {

        setup:

        def node1 = new NodeData(id: 11)
        def w1 = node1.createWorkerData( new JavaTestKit(system).getRef() )
        def w2 = node1.createWorkerData( new JavaTestKit(system).getRef() )
        def w3 = node1.createWorkerData( new JavaTestKit(system).getRef() )
        def w4 = node1.createWorkerData( new JavaTestKit(system).getRef() )
        dataStore.storeNode(node1)

        def node2 = new NodeData(id: 22)
        def w5 = node2.createWorkerData( new JavaTestKit(system).getRef() )
        def w6 = node2.createWorkerData( new JavaTestKit(system).getRef() )
        dataStore.storeNode(node2)

        dataStore.storeTask( new TaskEntry(TaskId.of(1), new TaskReq(script: 'do this')) )
        dataStore.storeTask( new TaskEntry(TaskId.of(2), new TaskReq(script: 'do that')) )
        dataStore.storeTask( new TaskEntry(TaskId.of(3), new TaskReq(script: 'do more')) )


        when:
        // create a render data - and - make a copy of it
        def block = new ScreenRenderer(11, dataStore)
        def copy = new ScreenRenderer(11, dataStore)
        def third = new ScreenRenderer(11, dataStore)

        // change some data in the copy
        copy.cluster.processedJobs = 2
        copy.cluster.failedJobs = 1

        copy.node.processed = 1
        copy.node.queueSize = 1

        copy.workers[0].jobId = 43
        copy.workers[0].jobAttempts = 2
        copy.workers[0].failed = 1

        // copy the copy against the original value
        copy.compareWith(block) { val -> ">> ${val} <<" }


        third.workers[0].jobId = 43
        third.workers[0].jobAttempts = 2
        third.workers[0].failed = 1

        third.compareWith(block) {}
        third.noCompare()

        // -- the changed value are wrapped by a string
        then:
        copy.cluster.allJobs == block.cluster.allJobs

        copy.cluster.processedJobs == ">> 2 <<"
        copy.cluster.failedJobs == ">> 1 <<"

        copy.node.failed == block.node.failed
        copy.node.processed == ">> 1 <<"
        copy.node.queueSize == ">> 1 <<"

        copy.workers[0].processed == block.workers[0].processed
        copy.workers[0].failed == ">> 1 <<"
        copy.workers[0].jobId == ">> 43 <<"
        copy.workers[0].jobAttempts == ">> 2 <<"

        third.workers[0].processed == block.workers[0].processed
        third.workers[0].failed == 1
        third.workers[0].jobId == 43
        third.workers[0].jobAttempts == 2

    }

    def "test render" () {

        setup:

        def node1 = new NodeData( id: 11 )
        def w1 = node1.createWorkerData( new JavaTestKit(system).getRef() )
        def w2 = node1.createWorkerData( new JavaTestKit(system).getRef() )
        def w3 = node1.createWorkerData( new JavaTestKit(system).getRef() )
        def w4 = node1.createWorkerData( new JavaTestKit(system).getRef() )
        dataStore.storeNode(node1)

        def node2 = new NodeData( id: 22 )
        def w5 = node2.createWorkerData( new JavaTestKit(system).getRef() )
        def w6 = node2.createWorkerData( new JavaTestKit(system).getRef() )
        dataStore.storeNode(node2)

        dataStore.storeTask( new TaskEntry(TaskId.of(1), new TaskReq(script: 'do this')) )
        dataStore.storeTask( new TaskEntry(TaskId.of(2), new TaskReq(script: 'do that')) )
        dataStore.storeTask( new TaskEntry(TaskId.of(3), new TaskReq(script: 'do more')) )


        when:
        // create a render data - and - make a copy of it
        def block = new ScreenRenderer(11, dataStore)
        def copy = new ScreenRenderer(11, dataStore)
        def third = new ScreenRenderer(11, dataStore)

        // change some data in the copy
        copy.cluster.processedJobs = 2
        copy.cluster.failedJobs = 1

        copy.node.processed = 1
        copy.node.queueSize = 1

        copy.workers[0].jobId = 43
        copy.workers[0].jobAttempts = 2
        copy.workers[0].failed = 1

        // copy the copy against the original value
        copy.compareWith(block) { val -> "*${val}*" }

        print copy.render()

        then:
        true


    }




}
