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
import circo.messages.WorkIsReady
import circo.messages.WorkToBeDone
import circo.messages.WorkToSpool
import circo.messages.WorkerCreated
import circo.messages.WorkerFailure
import circo.messages.WorkerRequestsWork
import circo.model.NodeData
import circo.model.NodeStatus
import circo.model.TaskEntry
import circo.model.TaskId
import circo.model.TaskReq
import circo.model.TaskResult
import circo.model.TaskStatus
import circo.model.WorkerData
import circo.model.WorkerRef
import circo.model.WorkerRefMock
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
            this.store.putNodeData(node)

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
        dataStore.saveTask(entry)

        final master = newTestActor(system, NodeMasterMock)
        final JavaTestKit probe = new JavaTestKit(system);

        final def worker1 = newWorkerProbe(system)
        final def worker2 = newWorkerProbe(system)
        final def worker3 = newWorkerProbe(system)

        // setup the workers map --> two are free and the last is busy
        // only the free workers will be notified
        master.getActor().node.putWorkerData(WorkerData.of(worker1))
        master.getActor().node.putWorkerData(WorkerData.of(worker2))
        master.getActor().node.putWorkerData(WorkerData.of(worker3) { it.currentTaskId = TaskId.of('99') } )

        when:
        // -- the message to add the Job to the processing queue
        master.tell( new WorkToSpool(theJobId), probe.getRef() )

        then:
        entry.ownerId == master.getActor().node.id
        master.underlyingActor().node.queue.poll() == theJobId
        worker1.probe.expectMsgEquals( WorkIsReady.getInstance() )
        worker2.probe.expectMsgEquals( WorkIsReady.getInstance() )
        worker3.probe.expectNoMsg()

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
        entry.worker = new WorkerRefMock( master1.actor.selfAddress.toString() + '/processor0' )
        dataStore.saveTask(entry)

        // setup the workers map --> two are free and the last is busy
        // only the free workers will be notified
        master1.actor.node.putWorkerData(WorkerData.of(workerRef1))
        master1.actor.node.putWorkerData(WorkerData.of(workerRef2))
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

        def node = dataStore.getNodeData(nodeId)

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
        dataStore.saveTask(new TaskEntry( TaskId.of('3'), new TaskReq(script:'Hello') ))
        dataStore.saveTask(new TaskEntry( TaskId.of('4'), new TaskReq(script:'Hello') ))

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
        dataStore.saveTask(job1)
        dataStore.saveTask(job2)
        dataStore.saveTask(job3)

        // this is a node with some work pending
        final masterWithWork = newTestActor(system, NodeMasterMock)
        masterWithWork.actor.node.queue.add(job1.id)
        masterWithWork.actor.node.queue.add(job2.id)
        masterWithWork.actor.node.queue.add(job3.id)
        dataStore.putNodeData(masterWithWork.actor.node)

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

    def "test WorkerRequestsWork when no local work BUT someone else HAS it (2)" () {

        setup:
        // some jobs
        def job1 = new TaskEntry(TaskId.of(1), new TaskReq())
        def job2 = new TaskEntry(TaskId.of(2), new TaskReq())
        def job3 = new TaskEntry(TaskId.of(3), new TaskReq())
        dataStore.saveTask(job1)
        dataStore.saveTask(job2)
        dataStore.saveTask(job3)

        // this is a node with some work pending
        final master1 = newTestActor(system, NodeMasterMock) { new NodeMasterMock(1, '1.1.1.1') }
        final master2 = newTestActor(system, NodeMasterMock) { new NodeMasterMock(2, '2.2.2.2') }

        master1.actor.allMasters.put( addr('2.2.2.2'), master2.actor.getSelf() )
        master2.actor.allMasters.put( addr('1.1.1.1'), master1.actor.getSelf() )

        // -- Master1 has some jobs to be processed
        master2.actor.node.queue.add(job1.id)
        master2.actor.node.queue.add(job2.id)
        master2.actor.node.queue.add(job3.id)
        dataStore.putNodeData(master2.actor.node)

        final worker1Probe = newProbe(system)


        when:
        master2.tell( new WorkerRequestsWork(worker1Probe.getRef()), worker1Probe.getRef() )

        then:
        master2.actor.node.queue.size() == 2

        //TODO ++ must be improved - Check also
        // - master1 has one job in its queue
        // - worker worker1 received WorkIsReady message
        //master1.underlyingActor().data.queue.size() == 1

        master1.probe.expectMsgEquals(new WorkToSpool(TaskId.of(1)))
        //worker1Probe.expectMsgEquals(WorkIsReady.getInstance())

    }

    def "test WorkerRequestsWork with failed update" () {

        setup:
        final task = new TaskEntry( TaskId.of(3), new TaskReq(script:'Hello') )
        dataStore.saveTask(task)


        final probe = newProbe(system)
        final master = newTestActor(system, NodeMasterMock )
        master.actor.node.queue.add(task.id)

        // -- The worker is not added to the NodeData
        //    So the worker request will fail and a message to re-queue the jobId is sent

        //master.underlyingActor().nodeData.createWorkerData(worker)

        when:
        master.tell( new WorkerRequestsWork(probe.getRef()) )

        then:
        // TODO ??? how expect message sent to itself ???
        //master.underlyingActor().data.queue.size() == 1
        master.probe.expectMsgEquals(WorkToSpool.of(task.id))

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
        dataStore.getNodeData(master.actor.nodeId).failed == 3
        dataStore.getNodeData(master.actor.nodeId).getWorkerData(worker1).failed == 2
        dataStore.getNodeData(master.actor.nodeId).getWorkerData(worker2).failed == 1
    }

    /*
     * When a member goes down unpredictably, check pending jobs that need
     * to be rescheduled
     */
    def void "test resumeJobs" () {

        setup:
        final master = newTestActor(system, NodeMasterMock)
        final senderProbe = new JavaTestKit(system)

        // this is SUCCESS, it must be notified to the sender
        def ticket1 = UUID.randomUUID()
        def job1 = new TaskEntry( TaskId.of('1'), new TaskReq(ticket: ticket1, script: '1') )
        job1.setSender(senderProbe.getRef())
        job1.result =  new TaskResult(taskId:job1.id, exitCode: 0)
        job1.ownerId = master.actor.nodeId
        def result1 = new ResultReply(ticket1, job1.result)

        // this is FAILED, it must be rescheduled
        def ticket2 = UUID.randomUUID()
        def job2 = new TaskEntry( TaskId.of('2'), new TaskReq(script: '2') )
        job2.setSender(senderProbe.getRef())
        job2.ownerId = master.actor.nodeId
        job2.result == new TaskResult(taskId:job2.id, exitCode: 127) // <-- the error condition
        def result2 = new ResultReply(ticket2, job2.result)

        // this is FAILED, BUT the number of attempts exceeded the maxAttempts,
        // so it must be notified to the sender
        def ticket3 = UUID.randomUUID()
        def job3 = new TaskEntry( TaskId.of('3'), new TaskReq(script: '3', maxAttempts: 2) )
        job3.status = TaskStatus.NEW
        job3.setSender(senderProbe.getRef())
        job3.ownerId = master.actor.nodeId

        dataStore.saveTask(job1)
        dataStore.saveTask(job2)
        dataStore.saveTask(job3)

        when:
        master.underlyingActor().resumeTasks( master.actor.selfAddress )

        then:
        // the NodeData for the dead node has been removed
        // so, getNodeData returns null
        dataStore.getNodeData(master.actor.nodeId).status == NodeStatus.DEAD

        // The job finished (successfully or with errors) are sent back to the sender
        senderProbe.expectMsgAllOf(result1)

        // The job not finished are retried, e.i. added to the spool queue
        // of the master node in this case
        master.probe.expectMsgAllOf( WorkToSpool.of(job2.id), WorkToSpool.of(job3.id) )

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
        dataStore.putNodeData(node1)

        // Node2 has THREE jobs in queue
        final probe2 = newProbe(system)
        final node2 = new NodeData( address: addr('2.2.2.2') )
        node2.status = NodeStatus.ALIVE
        node2.createWorkerData(probe2.getRef())
        node2.queue.add(job3.id)
        node2.queue.add(job4.id)
        node2.queue.add(job5.id)
        dataStore.putNodeData(node2)

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
