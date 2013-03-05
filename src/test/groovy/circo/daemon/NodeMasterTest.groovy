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

package circo.daemon
import static test.TestHelper.addr
import static test.TestHelper.newActor
import static test.TestHelper.newProbe
import static test.TestHelper.newTestActor
import static test.TestHelper.newWorkerProbe

import java.util.concurrent.atomic.AtomicInteger

import akka.testkit.JavaTestKit
import circo.messages.WorkIsDone
import circo.messages.WorkToBeDone
import circo.messages.WorkToSpool
import circo.messages.WorkerCreated
import circo.messages.WorkerFailure
import circo.messages.WorkerRequestsWork
import circo.model.Job
import circo.model.JobStatus
import circo.model.NodeData
import circo.model.NodeStatus
import circo.model.TaskEntry
import circo.model.TaskId
import circo.model.TaskReq
import circo.model.TaskResult
import circo.model.TaskStatus
import circo.model.WorkerData
import circo.model.WorkerRef
import circo.reply.ResultReply
import groovy.util.logging.Slf4j
import test.ActorSpecification
import test.TestHelper
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
class NodeMasterTest extends ActorSpecification {


    static class NodeMasterMock extends NodeMaster {

        static AtomicInteger index = new AtomicInteger()

        def NodeMasterMock( Integer nodeId = index.addAndGet(1), String address = null ) {
            super(NodeMasterTest.dataStore, nodeId )

            selfAddress = address ? addr(address) : TestHelper.randomAddress()
            this.node = new NodeData( id: nodeId, address: selfAddress );
            this.node.status = NodeStatus.ALIVE
            this.store.storeNode(node)

        }

        def void preStart() {
            log.debug "Members add [ $selfAddress : ${this.getSelf()} ]"
            allMasters.put( selfAddress, getSelf() )
        }

        def void postStop() { }

    }


    /*
     * When the Master receive a message 'AddWorkToQueue'
     * - a new job is queued
     * - the workers a notified with a message 'WorkIsReady'
     */
    def void "test AddWorkToQueue"() {

        setup:
        final theJobId = TaskId.of('2')
        final entry = new TaskEntry ( theJobId, new TaskReq(script: 'Hello') )
        dataStore.storeTask(entry)

        final master = newTestActor(system, NodeMasterMock)
        final JavaTestKit probe = new JavaTestKit(system);

        when:
        // -- the message to add the Job to the processing queue
        master.tell( new WorkToSpool(theJobId), probe.getRef() )

        then:
        dataStore.getTask(theJobId).ownerId == master.getActor().node.id
        dataStore.getTask(theJobId).status == TaskStatus.PENDING

        master.underlyingActor().node.queue.poll() == theJobId

    }

    /**
     * This test case try to schedule a new job for which does not exist
     * any entry in the database -- the message is discarded
     */
    def void "test AddWorkToQueue when missing entry"() {

        setup:
        final master = newTestActor(system, NodeMasterMock)
        final worker = newWorkerProbe(system)

        when:
        // setup the workers map
        // only the free workers will be notified
        master.actor.node.putWorkerData( WorkerData.of(worker) )

        // -- try to schedule a new TaskId to be process
        //    but no TaskEntry has been added
        master.tell( new WorkToSpool(TaskId.of(2)), worker.getActor() )


        then:
        master.actor.node.queue.isEmpty()
        worker.probe.expectNoMsg()

    }

    def void "test AddWorkToQueue for faulty job"() {

        setup:
        final def workerProbe1 = newProbe(system)
        final def workerProbe2 = newProbe(system)

        //
        // simulate a two node system
        //
        final master1 = newTestActor(system, NodeMasterMock)
        final master2 = newTestActor(system, NodeMasterMock)

        final def workerRef1 = new WorkerRef(workerProbe1.getRef())
        final def workerRef2 = new WorkerRef(workerProbe2.getRef())

        //
        // this is a job that has been processed on node1
        // (since the 'worker' attribute contains a path relative to the address of master1)
        //
        // so when it is scheduled again in 'master1' it will send it to 'master2'
        //
        def theJobId = TaskId.of('2')
        def entry = new TaskEntry ( theJobId, new TaskReq(script: 'Hello') )
        entry.worker = workerRef1
        dataStore.storeTask(entry)

        // setup the workers map --> two are free and the last is busy
        // only the free workers will be notified
        master1.actor.node.createWorkerData(workerRef1)
        master1.actor.node.createWorkerData(workerRef2)
        master1.actor.allMasters.put( master2.actor.selfAddress, master2.actor.getSelf() )

        when:
        // -- the message to add the task to the processing queue
        //    since this job has already failed on Master1
        //    it is expected that will be forwarded to 'another' master
        master1.tell( new WorkToSpool(theJobId) )

        then:
        // no worker receive any message
        master1.actor.node.queue.isEmpty()
        workerProbe1.expectNoMsg()
        workerProbe2.expectNoMsg()

        // ! the 'WorkToSpool' message is redirected to the master2
        master2.probe.expectMsgEquals(new WorkToSpool(theJobId))

    }


    /**
     * When a new JobProcess actor is create is send a 'WorkerCreated' message,
     * the master add it in the {@code NodeMaster#workersMap}
     */
    def "test WorkerCreated" () {

        setup:
        final nodeId = 1
        final master = newTestActor(system, NodeMasterMock) { new NodeMasterMock(nodeId) }

        final def workerProbe1 = new JavaTestKit(system)
        final def workerProbe2 = new JavaTestKit(system)

        when:
        master.tell( new WorkerCreated( workerProbe1.getRef() ) )
        master.tell( new WorkerCreated( workerProbe2.getRef() ) )

        def node = dataStore.getNode(nodeId)

        then:
        node.workers.size() == 2
        node.hasWorkerData(new WorkerRef(workerProbe1.getRef()) )
        node.hasWorkerData(new WorkerRef(workerProbe2.getRef()) )

    }

    /**
     * A worker send a message 'WorkerRequestsWork' to the master to request for a task to be processed
     * The master will reply to it with a message 'WorkToBeDone'
     */
    def "test WorkerRequestsWork" () {

        setup:
        dataStore.storeTask(new TaskEntry( TaskId.of('3'), new TaskReq(script:'Hello') ))
        dataStore.storeTask(new TaskEntry( TaskId.of('4'), new TaskReq(script:'Hello') ))

        final master = newTestActor(system, NodeMasterMock )
        final def probe = new JavaTestKit(system)
        final worker = new WorkerRef(probe.getRef())

        master.underlyingActor().node.createWorkerData(worker)
        master.underlyingActor().node.queue.add( TaskId.of('3') )
        master.underlyingActor().node.queue.add( TaskId.of('4') )

        when:
        master.tell( new WorkerRequestsWork(probe.getRef()), probe.getRef() )

        then:
        probe.expectMsgEquals( new WorkToBeDone( TaskId.of('3') ) )
        master.underlyingActor().node.getWorkerData(worker).currentTaskId == TaskId.of('3')
        master.underlyingActor().node.queue.size() == 1


    }


    def "test WorkerRequestsWork when NO WORK"() {

        setup:
        // create the NodeMaster actor
        final master = newTestActor(system, NodeMasterMock )

        // the worker
        final def probe = newProbe(system)
        final worker = new WorkerRef(probe.getRef())
        master.actor.node.createWorkerData(worker)

        when:
        master.tell( new WorkerRequestsWork(probe.getRef()), probe.getRef() )

        then:
        probe.expectNoMsg()
        master.probe.expectNoMsg()
    }

    def "test WorkerRequestsWork when no local work BUT someone else HAS it" () {

        setup:
        // some jobs
        def job1 = new TaskEntry(TaskId.of(1), new TaskReq())
        def job2 = new TaskEntry(TaskId.of(2), new TaskReq())
        def job3 = new TaskEntry(TaskId.of(3), new TaskReq())
        dataStore.storeTask(job1)
        dataStore.storeTask(job2)
        dataStore.storeTask(job3)

        // this is a node with some work pending
        final masterWithWork = newTestActor(system, NodeMasterMock)
        masterWithWork.actor.node.queue.add(job1.id)
        masterWithWork.actor.node.queue.add(job2.id)
        masterWithWork.actor.node.queue.add(job3.id)
        dataStore.storeNode(masterWithWork.actor.node)

        // this Master HAS NO work, but receive a message from a worker
        final master = newTestActor(system, NodeMasterMock)
        final workerProbe = newProbe(system)
        final workerRef = new WorkerRef(workerProbe.getRef())
        master.actor.node.createWorkerData(workerRef)
        master.actor.allMasters.put( masterWithWork.actor.selfAddress, masterWithWork.probe.getRef() )

        when:
        master.tell( new WorkerRequestsWork(workerRef), workerProbe.getRef() )

        then:
        masterWithWork.probe.expectMsgEquals(new WorkerRequestsWork(workerRef))
        workerProbe.expectNoMsg()

    }

    /*
     * TODO +++ fix
     */
    def "test WorkerRequestsWork when no local work BUT someone else HAS it (2)" () {

        setup:
        // some jobs
        def job1 = new TaskEntry(TaskId.of(1), new TaskReq())
        def job2 = new TaskEntry(TaskId.of(2), new TaskReq())
        def job3 = new TaskEntry(TaskId.of(3), new TaskReq())
        dataStore.storeTask(job1)
        dataStore.storeTask(job2)
        dataStore.storeTask(job3)

        // this is a node with some work pending
        final master1 = newTestActor(system, NodeMasterMock) { new NodeMasterMock(1, '1.1.1.1') }
        final master2 = newTestActor(system, NodeMasterMock) { new NodeMasterMock(2, '2.2.2.2') }

        master1.actor.allMasters.put( addr('2.2.2.2'), master2.actor.getSelf() )
        master2.actor.allMasters.put( addr('1.1.1.1'), master1.actor.getSelf() )

        final worker1Probe = newProbe(system)
        master1.actor.node.createWorkerData(worker1Probe.getRef())

        // -- Master1 has some jobs to be processed
        master2.actor.node.queue.add(job1.id)
        master2.actor.node.queue.add(job2.id)
        master2.actor.node.queue.add(job3.id)
        dataStore.storeNode(master2.actor.node)


        when:
        /*
         * the 'master1' is asked for tasks from its one worker
         * but it doesn't have
         */
        master1.tell( new WorkerRequestsWork(worker1Probe.getRef()), worker1Probe.getRef() )

        then:
        /*
         * the request is forwarded to 'Master2'
         */
        master2.probe.expectMsgClass(WorkerRequestsWork)


    }


    /*
     * Worker send a message 'WorkIsDone' to signal that is has processed the
     * assigned job
     */
    def "test WorkIsDone" () {

        setup:
        final jobId = TaskId.of( '123' )
        final entry = new TaskEntry( jobId, new TaskReq() )
        entry.result = new TaskResult()

        final master = newTestActor(system, NodeMasterMock)

        final def probe = new JavaTestKit(system)
        final worker = new WorkerRef(probe.getRef())

        master.actor.node.createWorkerData(worker)
        master.actor.node.assignTaskId(worker, jobId)

        when:
        master.tell ( new WorkIsDone(worker, jobId) )

        then:
        master.actor.node.getWorkerData(worker).currentTaskId == null

    }

    /*
     * A processor send the message to the master actor to signal a error
     *
     */
    def "test WorkerFailure" () {

        setup:
        final master = newTestActor(system, NodeMasterMock )
        final worker1 = new WorkerRef(new JavaTestKit(system).getRef())
        final worker2 = new WorkerRef(new JavaTestKit(system).getRef())
        master.actor.node.createWorkerData(worker1)
        master.actor.node.createWorkerData(worker2)

        when:
        master.tell( new WorkerFailure(worker1) )
        master.tell( new WorkerFailure(worker1) )
        master.tell( new WorkerFailure(worker2) )

        then:
        dataStore.getNode(master.actor.nodeId).failed == 3
        dataStore.getNode(master.actor.nodeId).getWorkerData(worker1).failed == 2
        dataStore.getNode(master.actor.nodeId).getWorkerData(worker2).failed == 1
    }

    /*
     * When a member goes down unpredictably, check pending jobs that need
     * to be rescheduled
     */

    def void "test resumeJobs" () {

        setup:
        /*
         * there are two master nodes in this cluster
         */
        final master1 = newTestActor(system, NodeMasterMock)
        final master2 = newTestActor(system, NodeMasterMock)
        final senderProbe = new JavaTestKit(system)
        def reqId1 = UUID.randomUUID()
        def reqId2 = UUID.randomUUID()
        def reqId3 = UUID.randomUUID()
        def reqId4 = UUID.randomUUID()

        /*
         * the following tasks are assigned to 'master1'
         */

        // this is SUCCESS, it must be notified to the sender
        def task1 = new TaskEntry( TaskId.of('1'), new TaskReq(requestId: reqId1, script: '1') )
        task1.setSender(senderProbe.getRef())
        task1.result =  new TaskResult(taskId: task1.id, exitCode: 0)
        task1.ownerId = master1.actor.nodeId
        task1.req.notifyResult = true
        def result1 = new ResultReply(reqId1, task1.result)

        // this is FAILED, it must be rescheduled
        def task2 = new TaskEntry( TaskId.of('2'), new TaskReq(requestId: reqId2, script: '2') )
        task2.setSender(senderProbe.getRef())
        task2.ownerId = master1.actor.nodeId
        task2.result == new TaskResult(taskId: task2.id, exitCode: 127) // <-- the error condition

        // this is FAILED, BUT the number of attempts exceeded the maxAttempts,
        // so it must be notified to the sender
        def task3 = new TaskEntry( TaskId.of('3'), new TaskReq(requestId: reqId3, script: '3', maxAttempts: 2) )
        task3.status = TaskStatus.NEW
        task3.setSender(senderProbe.getRef())
        task3.ownerId = master1.actor.nodeId


        /*
         * this task belongs to 'master2'
         */
        def task4 = new TaskEntry( TaskId.of('4'), new TaskReq(requestId: reqId4, script: '4') )
        task4.status = TaskStatus.NEW
        task4.setSender(senderProbe.getRef())
        task4.ownerId = master2.actor.nodeId

        dataStore.storeTask(task1)
        dataStore.storeTask(task2)
        dataStore.storeTask(task3)
        dataStore.storeTask(task4)

        dataStore.addToSink(task1)
        dataStore.addToSink(task2)
        dataStore.addToSink(task3)
        dataStore.addToSink(task4)

        def job = new Job( reqId1 )
        job.status = JobStatus.PENDING
        dataStore.storeJob( job )

        master2.actor.node.queue << task4.id

        when:
        /*
         * master1 dies the other node (master2) will handle the situation
         */
        master2.underlyingActor().manageMemberDowned( master1.actor.selfAddress )


        then:
        // the NodeData for the dead node has been removed
        // so, getNodeData returns null
        dataStore.getNode(master1.actor.nodeId).status == NodeStatus.DEAD

        // The job finished (successfully or with errors) are sent back to the sender
        senderProbe.expectMsgAllOf(result1)

        // the recovered tasks now belongs to 'master2'
        task2.ownerId == master2.actor.nodeId
        task3.ownerId == master2.actor.nodeId
        task4.ownerId == master2.actor.nodeId

        // they are queued into 'master2' queue
        master2.actor.node.queue.toSet() == [task2.id, task3.id, task4.id] as Set

    }


    def "test getAny and someoneElse with one member" () {

        setup:
        def master = newActor(system, NodeMasterMock)

        when:
        def actor1 = master.getAny()
        def actor2 = master.someoneElse()

        then:
        master.allMasters.size() == 1
        master.getSelf() == actor1
        master.getSelf() == actor2
    }

    def "test any - someoneElse - someoneWithWork with multiple members" () {

        setup:
        def probe1 = newProbe(system)
        def probe2 = newProbe(system)
        def master = newActor(system, NodeMasterMock)
        master.allMasters.put( addr('1.1.1.1'), probe1.getRef() )
        master.allMasters.put( addr('2.2.2.2'), probe2.getRef() )

        when:
        // with multiple members 'getAny' may return itself
        def anyList = []
        5.times { anyList << master.getAny() }

        // with multiple members 'someoneElse' WONT return itself
        def someoneList = []
        5.times { someoneList << master.someoneElse() }

        then:
        master.someoneWithWork() == null
        master.allMasters.size() == 3
        master.getSelf() in anyList
        !(master.getSelf() in someoneList)


    }


    def "test someoneElseWithWork" () {

        setup:
        // 5 jobs
        def job1 = new TaskEntry(TaskId.of(1), new TaskReq(script: '1'))
        def job2 = new TaskEntry(TaskId.of(2), new TaskReq(script: '2'))
        def job3 = new TaskEntry(TaskId.of(3), new TaskReq(script: '3'))
        def job4 = new TaskEntry(TaskId.of(4), new TaskReq(script: '4'))
        def job5 = new TaskEntry(TaskId.of(5), new TaskReq(script: '5'))

        // Node1 has TWO jobs in queue
        final probe1 = newProbe(system)
        final node1 = new NodeData( address: addr('1.1.1.1') )
        node1.status = NodeStatus.ALIVE
        node1.createWorkerData(probe1.getRef())
        node1.queue.add(job1.id)
        node1.queue.add(job2.id)
        dataStore.storeNode(node1)

        // Node2 has THREE jobs in queue
        final probe2 = newProbe(system)
        final node2 = new NodeData( address: addr('2.2.2.2') )
        node2.status = NodeStatus.ALIVE
        node2.createWorkerData(probe2.getRef())
        node2.queue.add(job3.id)
        node2.queue.add(job4.id)
        node2.queue.add(job5.id)
        dataStore.storeNode(node2)

        final master = newActor(system, NodeMasterMock)
        master.allMasters.put( addr('1.1.1.1'), probe1.getRef() )
        master.allMasters.put( addr('2.2.2.2'), probe2.getRef() )


        when:
        // master has NO jobs in queue BUT look for someone else that has a job
        def someActor = master.someoneWithWork()


        then:
        // it must be 'probe2' with one with THREE jobs
        someActor == probe2.getRef()


    }




}
