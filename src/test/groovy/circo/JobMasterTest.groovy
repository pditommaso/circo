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

package circo
import akka.testkit.JavaTestKit
import groovy.util.logging.Slf4j
import circo.data.*
import circo.messages.*
import test.ActorSpecification

import static test.TestHelper.*
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
class JobMasterTest extends ActorSpecification {


    static class JobMasterMock extends JobMaster {

        def JobMasterMock(String address = null) {
            super(JobMasterTest.dataStore)

            selfAddress = addr(address)
            node = new NodeData( address: selfAddress );
            store.putNodeData(node)
        }

        def void preStart() {
            log.debug "Members add [ $selfAddress : ${this.getSelf()} ]"
            members.put( selfAddress, getSelf() )
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
        final theJobId = JobId.of('2')
        final entry = new JobEntry ( theJobId, new JobReq(script: 'Hello') )
        dataStore.saveJob(entry)

        final master = newTestActor(system, JobMasterMock)
        final JavaTestKit probe = new JavaTestKit(system);

        final def worker1 = newWorkerProbe(system)
        final def worker2 = newWorkerProbe(system)
        final def worker3 = newWorkerProbe(system)

        // setup the workers map --> two are free and the last is busy
        // only the free workers will be notified
        master.getActor().node.putWorkerData(WorkerData.of(worker1))
        master.getActor().node.putWorkerData(WorkerData.of(worker2))
        master.getActor().node.putWorkerData(WorkerData.of(worker3) { it.currentJobId = JobId.of('99') } )

        when:
        // -- the message to add the Job to the processing queue
        master.tell( new WorkToSpool(theJobId), probe.getRef() )

        then:
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
        final master = newTestActor(system, JobMasterMock)
        final worker = newWorkerProbe(system)

        when:
        // setup the workers map
        // only the free workers will be notified
        master.actor.node.putWorkerData( WorkerData.of(worker) )

        // -- try to schedule a new JobId to be process
        //    but no JobEntry has been added
        master.tell( new WorkToSpool(JobId.of(2)), worker.getActor() )


        then:
        master.actor.node.queue.isEmpty()
        worker.probe.expectNoMsg()

    }

    def void "test AddWorkToQueue for faulty job"() {

        setup:
        final def workerProbe1 = newProbe(system)
        final def workerProbe2 = newProbe(system)

        // the faulty job
        def theJobId = JobId.of('2')
        def entry = new JobEntry ( theJobId, new JobReq(script: 'Hello') )
        entry.worker = new WorkerRefMock('/user/worker')
        dataStore.saveJob(entry)


        final master1 = newTestActor(system, JobMasterMock)
        final master2 = newTestActor(system, JobMasterMock) { new JobMasterMock('2.2.2.2') }

        final def workerRef1 = new WorkerRef(workerProbe1.getRef())
        final def workerRef2 = new WorkerRef(workerProbe2.getRef())

        // setup the workers map --> two are free and the last is busy
        // only the free workers will be notified
        master1.actor.node.putWorkerData(WorkerData.of(workerRef1))
        master1.actor.node.putWorkerData(WorkerData.of(workerRef2))
        master1.actor.members.put( addr('2.2.2.2'), master2.actor.getSelf() )


        when:
        // -- the message to add the Job to the processing queue
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
     * the master add it in the {@code JobMaster#workersMap}
     */
    def "test WorkerCreated" () {

        setup:
        final master = newTestActor(system, JobMasterMock) { new JobMasterMock('1.1.1.1') }

        final def workerProbe1 = new JavaTestKit(system)
        final def workerProbe2 = new JavaTestKit(system)

        when:
        master.tell( new WorkerCreated( workerProbe1.getRef() ) )
        master.tell( new WorkerCreated( workerProbe2.getRef() ) )

        def node = dataStore.getNodeData(addr('1.1.1.1'))

        then:
        node.workers.size() == 2
        node.hasWorkerData(new WorkerRef(workerProbe1.getRef()) )
        node.hasWorkerData(new WorkerRef(workerProbe2.getRef()) )

    }

    /**
     * A worker send a message 'WorkerRequestsWork' to the master to request a job to be processed
     * The master will reply to it with a message 'WorkToBeDone'
     */
    def "test WorkerRequestsWork" () {

        setup:
        dataStore.saveJob(new JobEntry( JobId.of('3'), new JobReq(script:'Hello') ))
        dataStore.saveJob(new JobEntry( JobId.of('4'), new JobReq(script:'Hello') ))


        final master = newTestActor(system, JobMasterMock )
        final def probe = new JavaTestKit(system)
        final worker = new WorkerRef(probe.getRef())

        master.underlyingActor().node.createWorkerData(worker)
        master.underlyingActor().node.queue.add( JobId.of('3') )
        master.underlyingActor().node.queue.add( JobId.of('4') )

        when:
        master.tell( new WorkerRequestsWork(probe.getRef()), probe.getRef() )

        then:
        probe.expectMsgEquals( new WorkToBeDone( JobId.of('3') ) )
        master.underlyingActor().node.getWorkerData(worker).currentJobId == JobId.of('3')
        master.underlyingActor().node.queue.size() == 1


    }


    def "test WorkerRequestsWork when NO WORK"() {

        setup:
        // create the JobMaster actor
        final master = newTestActor(system, JobMasterMock )

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
        def job1 = new JobEntry(JobId.of(1), new JobReq())
        def job2 = new JobEntry(JobId.of(2), new JobReq())
        def job3 = new JobEntry(JobId.of(3), new JobReq())
        dataStore.saveJob(job1)
        dataStore.saveJob(job2)
        dataStore.saveJob(job3)

        // this is a node with some work pending
        final masterWithWork = newTestActor(system, JobMasterMock) { new JobMasterMock('2.2.2.2') }
        masterWithWork.actor.node.queue.add(job1.id)
        masterWithWork.actor.node.queue.add(job2.id)
        masterWithWork.actor.node.queue.add(job3.id)
        dataStore.putNodeData(masterWithWork.actor.node)

        // this Master HAS NO work, but receive a message from a worker
        final master = newTestActor(system, JobMasterMock)
        final workerProbe = newProbe(system)
        final workerRef = new WorkerRef(workerProbe.getRef())
        master.actor.members.put( addr('2.2.2.2'), masterWithWork.probe.getRef() )

        when:
        master.tell( new WorkerRequestsWork(workerRef), workerProbe.getRef() )

        then:
        masterWithWork.probe.expectMsgEquals(new WorkerRequestsWork(workerRef))
        workerProbe.expectNoMsg()

    }

    def "test WorkerRequestsWork when no local work BUT someone else HAS it (2)" () {

        setup:
        // some jobs
        def job1 = new JobEntry(JobId.of(1), new JobReq())
        def job2 = new JobEntry(JobId.of(2), new JobReq())
        def job3 = new JobEntry(JobId.of(3), new JobReq())
        dataStore.saveJob(job1)
        dataStore.saveJob(job2)
        dataStore.saveJob(job3)

        // this is a node with some work pending
        final master1 = newTestActor(system, JobMasterMock) { new JobMasterMock('1.1.1.1') }
        final master2 = newTestActor(system, JobMasterMock) { new JobMasterMock('2.2.2.2') }

        master1.actor.members.put( addr('2.2.2.2'), master2.actor.getSelf() )
        master2.actor.members.put( addr('1.1.1.1'), master1.actor.getSelf() )

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

        master1.probe.expectMsgEquals(new WorkToSpool(JobId.of(1)))
        //worker1Probe.expectMsgEquals(WorkIsReady.getInstance())

    }

    def "test WorkerRequestsWork with failed update" () {

        setup:
        final job = new JobEntry( JobId.of(3), new JobReq(script:'Hello') )
        dataStore.saveJob(job)


        final probe = newProbe(system)
        final master = newTestActor(system, JobMasterMock )
        master.actor.node.queue.add(job.id)

        // -- The worker is not added to the NodeData
        //    So the worker request will fail and a message to re-queue the jobId is sent

        //master.underlyingActor().nodeData.createWorkerData(worker)

        when:
        master.tell( new WorkerRequestsWork(probe.getRef()) )

        then:
        // TODO ??? how expect message sent to itself ???
        //master.underlyingActor().data.queue.size() == 1
        master.probe.expectMsgEquals(WorkToSpool.of(job.id))

    }

    /*
     * Worker send a message 'WorkIsDone' to signal that is has processed the
     * assigned job
     */
    def "test WorkIsDone" () {

        setup:
        final jobId = JobId.of( '123' )
        final entry = new JobEntry( jobId, new JobReq() )
        entry.result = new JobResult(success: true)

        final master = newTestActor(system, JobMasterMock)

        final def probe = new JavaTestKit(system)
        final worker = new WorkerRef(probe.getRef())

        master.actor.node.createWorkerData(worker)
        master.actor.node.assignJobId(worker, jobId)

        when:
        master.tell ( new WorkIsDone( worker, entry ) )

        then:
        master.actor.node.getWorkerData(worker).currentJobId == null

    }

    /*
     * A processor send the message to the master actor to signal a error
     *
     */
    def "test WorkerFailure" () {

        setup:
        final master = newTestActor(system, JobMasterMock )
        final worker1 = new WorkerRef(new JavaTestKit(system).getRef())
        final worker2 = new WorkerRef(new JavaTestKit(system).getRef())
        master.actor.node.createWorkerData(worker1)
        master.actor.node.createWorkerData(worker2)

        when:
        master.tell( new WorkerFailure(worker1) )
        master.tell( new WorkerFailure(worker1) )
        master.tell( new WorkerFailure(worker2) )

        then:
        dataStore.getNodeData(defaultAddr()).failed == 3
        dataStore.getNodeData(defaultAddr()).getWorkerData(worker1).failed == 2
        dataStore.getNodeData(defaultAddr()).getWorkerData(worker2).failed == 1
    }

    /*
     * When a member goes down unpredictably, check peding jobs that need
     * to be rescheduled
     */
    def void "test resumeJobs" () {

        setup:
        final master = newTestActor(system, JobMasterMock)
        final senderProbe = new JavaTestKit(system)
        final worker1 = new JavaTestKit(system)
        final worker2 = new JavaTestKit(system)
        final worker3 = new JavaTestKit(system)
        final worker4 = new JavaTestKit(system)

        // this is SUCCESS, it must be notified to the sender
        def result1
        def job1 = new JobEntry( JobId.of('1'), new JobReq(script: '1') )
        job1.setSender(senderProbe.getRef())
        job1.result = result1 =  new JobResult(success:true, jobId:job1.id)

        // this is FAILED, it must be rescheduled
        def result2
        def job2 = new JobEntry( JobId.of('2'), new JobReq(script: '2') )
        job2.setSender(senderProbe.getRef())
        job2.result = result2 = new JobResult(success: false, jobId:job2.id)

        // this is FAILED, BUT th number of attempts exceeded the maxAttempts,
        // so it must be notified to the sender
        def result3
        def job3 = new JobEntry( JobId.of('3'), new JobReq(script: '3', maxAttempts: 2) )
        job3.attempts = 3
        job3.setSender(senderProbe.getRef())
        job3.result = result3 = new JobResult(success: false, jobId:job3.id)


        dataStore.saveJob(job1)
        dataStore.saveJob(job2)
        dataStore.saveJob(job3)


        def node = new NodeData(address:addr('1.1.1.1'))
        node.createWorkerData( worker1.getRef() )
        node.createWorkerData( worker2.getRef() )
        node.createWorkerData( worker3.getRef() )
        node.createWorkerData( worker4.getRef() )

        node.assignJobId( worker1.getRef(), job1.id  )
        node.assignJobId( worker2.getRef(), job2.id  )
        node.assignJobId( worker3.getRef(), job3.id  )
        dataStore.putNodeData(node)

        when:
        master.underlyingActor().resumeJobs( addr('1.1.1.1') )

        then:
        // the NodeData for the dead node has been remove
        // so, getNodeData returns null
        dataStore.getNodeData(addr('1.1.1.1')) == null

        // The job finished (successfully or with errors) are sent back to the sender
        senderProbe.expectMsgAllOf(result1, result3)

        // The job not finished are retried, e.i. added to the spool queue
        // of the master node in this case
        master.probe.expectMsgEquals( WorkToSpool.of(job2.id) )

    }


    def "test getAny and someoneElse with one member" () {

        setup:
        def master = newActor(system, JobMasterMock)

        when:
        def actor1 = master.getAny()
        def actor2 = master.someoneElse()

        then:
        master.members.size() == 1
        master.getSelf() == actor1
        master.getSelf() == actor2
    }

    def "test any - someoneElse - someoneWithWork with multiple members" () {

        setup:
        def probe1 = newProbe(system)
        def probe2 = newProbe(system)
        def master = newActor(system, JobMasterMock)
        master.members.put( addr('1.1.1.1'), probe1.getRef() )
        master.members.put( addr('2.2.2.2'), probe2.getRef() )

        when:
        // with multiple members 'getAny' may return itself
        def anyList = []
        5.times { anyList << master.getAny() }

        // with multiple members 'someoneElse' WONT return itself
        def someoneList = []
        5.times { someoneList << master.someoneElse() }

        then:
        master.someoneWithWork() == null
        master.members.size() == 3
        master.getSelf() in anyList
        !(master.getSelf() in someoneList)


    }


    def "test someoneElseWithWork" () {

        setup:
        // 5 jobs
        def job1 = new JobEntry(JobId.of(1), new JobReq(script: '1'))
        def job2 = new JobEntry(JobId.of(2), new JobReq(script: '2'))
        def job3 = new JobEntry(JobId.of(3), new JobReq(script: '3'))
        def job4 = new JobEntry(JobId.of(4), new JobReq(script: '4'))
        def job5 = new JobEntry(JobId.of(5), new JobReq(script: '5'))

        // Node1 has TWO jobs in queue
        final probe1 = newProbe(system)
        final node1 = new NodeData( address: addr('1.1.1.1') )
        node1.createWorkerData(probe1.getRef())
        node1.queue.add(job1.id)
        node1.queue.add(job2.id)
        dataStore.putNodeData(node1)

        // Node2 has THREE jobs in queue
        final probe2 = newProbe(system)
        final node2 = new NodeData( address: addr('2.2.2.2') )
        node2.createWorkerData(probe2.getRef())
        node2.queue.add(job3.id)
        node2.queue.add(job4.id)
        node2.queue.add(job5.id)
        dataStore.putNodeData(node2)

        final master = newActor(system, JobMasterMock)
        master.members.put( addr('1.1.1.1'), probe1.getRef() )
        master.members.put( addr('2.2.2.2'), probe2.getRef() )


        when:
        // master has NO jobs in queue BUT look for someone else that has a job
        def someActor = master.someoneWithWork()


        then:
        // it must be 'probe2' with one with THREE jobs
        someActor == probe2.getRef()


    }


}
