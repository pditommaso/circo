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
import static test.TestHelper.newProbe
import static test.TestHelper.newTestActor

import circo.client.CmdList
import circo.client.CmdNode
import circo.client.CmdSub
import circo.model.Context
import circo.model.Job
import circo.model.JobStatus
import circo.model.NodeDataTest
import circo.model.TaskEntry
import circo.model.TaskId
import circo.model.TaskResult
import circo.model.TaskStatus
import circo.reply.ListReply
import circo.reply.NodeReply
import circo.reply.SubReply
import scala.concurrent.duration.Duration
import test.ActorSpecification
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class FrontEndTest extends ActorSpecification {

    def 'test cmd sub' () {

        setup:
        def sender = newProbe(test.ActorSpecification.system)
        def master = newProbe(test.ActorSpecification.system)
        def frontend = newTestActor(test.ActorSpecification.system,FrontEnd) {
            def FE = new FrontEnd(test.ActorSpecification.dataStore)
            FE.master = master.getRef()
            return FE
        }

        final ticket = UUID.randomUUID()
        final sub = new CmdSub()
        sub.ticket = ticket
        sub.command = ['echo', 'Hello world']
        sub.env.put('VAR1', 'val_1')
        sub.env.put('VAR2', 'val_2')
        sub.maxDuration = Duration.create('1 min')
        sub.maxAttempts = 4


        when:
        frontend.tell(sub, sender.getRef())

        /*
         * this should make part of the 'expected' results
         */

        // the sender get a 'reply' message
        def result = sender.expectMsgClass(SubReply)
        // the reply contains the ID(s) of the new submitted job(s)
        def ID = result.taskIds.get(0)
        // the a new job entry has been created
        def entry = test.ActorSpecification.dataStore.getTask( ID )

        then:
        result.taskIds.size() == 1
        result.ticket == sub.ticket
        result.messages.size() == 0

        entry.creationTime != null
        entry.req.maxAttempts == sub.maxAttempts
        entry.req.maxDuration == sub.maxDuration.toMillis()
        entry.req.environment.each{ k, v -> sub.env.get(k) == v }
        entry.req.environment['JOB_ID'] == ticket.toString()
        entry.req.environment['TASK_ID'] == ID.toFmtString()
        //entry.req.environment == sub.env
        entry.req.script == sub.command.join(' ')


    }


    def 'test cmd sub --each' () {

        setup:
        def sender = newProbe(test.ActorSpecification.system)
        def master = newProbe(test.ActorSpecification.system)
        def frontend = newTestActor(test.ActorSpecification.system,FrontEnd) {
            def FE = new FrontEnd(test.ActorSpecification.dataStore)
            FE.master = master.getRef()
            return FE
        }

        final ticket = UUID.randomUUID()
        final sub = new CmdSub()
        sub.ticket = ticket
        sub.command = ['echo', 'Hello world']
        // the context contains two variables
        // - X == 1..2
        // - Y == [ alpha, beta ]
        sub.context = new Context().put('X','1..2').put('Y','[alpha,beta]')

        // submit for each values in the (X,Y) pair, so there ae 4 combinations
        sub.eachItems = ['X','Y']

        when:
        frontend.tell(sub, sender.getRef())

        // the a new job entry has been created
        def result = sender.expectMsgClass(SubReply)
        def entry0 = test.ActorSpecification.dataStore.getTask( result.taskIds[0] )
        def entry1 = test.ActorSpecification.dataStore.getTask( result.taskIds[1] )
        def entry2 = test.ActorSpecification.dataStore.getTask( result.taskIds[2] )
        def entry3 = test.ActorSpecification.dataStore.getTask( result.taskIds[3] )

        then:
        result.taskIds.size() == 4
        result.taskIds[0] == TaskId.of(1)
        entry0.req.environment['TASK_ID'] == entry0.id.toFmtString()
        entry0.req.context.getData('X') == '1'
        entry0.req.context.getData('Y') == 'alpha'

        entry1.req.environment['TASK_ID'] == entry1.id.toFmtString()
        entry1.req.context.getData('X') == '2'
        entry1.req.context.getData('Y') == 'alpha'

        entry2.req.environment['TASK_ID'] == entry2.id.toFmtString()
        entry2.req.context.getData('X') == '1'
        entry2.req.context.getData('Y') == 'beta'

        entry3.req.environment['TASK_ID'] == entry3.id.toFmtString()
        entry3.req.context.getData('X') == '2'
        entry3.req.context.getData('Y') == 'beta'

    }




    def 'text cmd list jobs' () {

        setup:
        final requestId1 = UUID.randomUUID()
        final requestId2 = UUID.randomUUID()

        // first request
        final task1 = TaskEntry.create(1)  { TaskEntry it -> it.req.requestId = requestId1; it.req.script = 'Hello'; it.attempts=1; }
        final task2 = TaskEntry.create(2)  { TaskEntry it -> it.req.requestId = requestId1; it.req.script = 'Hello'; it.attempts=1; it.status = TaskStatus.TERMINATED }

        // second request
        final task3 = TaskEntry.create(3)  { TaskEntry it -> it.req.requestId = requestId2; it.req.script = 'Ciao'; it.attempts=1; }
        final task4 = TaskEntry.create(4)  { TaskEntry it -> it.req.requestId = requestId2; it.req.script = 'Ciao'; it.attempts=1; it.status = TaskStatus.TERMINATED }
        final task5 = TaskEntry.create(5)  { TaskEntry it -> it.req.requestId = requestId2; it.req.script = 'Ciao'; it.attempts=2; it.result = new TaskResult(exitCode: 1) }

        dataStore.storeTask(task1)
        dataStore.storeTask(task2)
        dataStore.storeTask(task3)
        dataStore.storeTask(task4)
        dataStore.storeTask(task5)


        /*
         * the jobs
         */
        final job1 = Job.create(requestId1)
        final job2 = Job.create(requestId2) { Job it -> it.status = JobStatus.ERROR }

        dataStore.storeJob(job1)
        dataStore.storeJob(job2)


        /*
         * prepare the command
         */
        def sender = newProbe(test.ActorSpecification.system)
        def frontend = newTestActor(test.ActorSpecification.system,FrontEnd) { new FrontEnd(test.ActorSpecification.dataStore) }

        // -- first command - LIST ALL
        frontend.tell( new CmdList(all: true), sender.getRef() )
        when:
        def result1 = sender.expectMsgClass(ListReply)

        then:
        result1.jobs.size() == 2
        result1.jobs.find { it.requestId == requestId1 }.command == 'Hello'
        result1.jobs.find { it.requestId == requestId2 }.command == 'Ciao'

        result1.jobs.find { it.requestId == requestId1 }.numOfFailedTasks == 0
        result1.jobs.find { it.requestId == requestId1 }.numOfPendingTasks == 1

        result1.jobs.find { it.requestId == requestId2 }.numOfFailedTasks == 1
        result1.jobs.find { it.requestId == requestId2 }.numOfPendingTasks == 1

        // -- second command - LIST PENDING
        when:
        frontend.tell( new CmdList(status: ['error']), sender.getRef() )
        def result2 = sender.expectMsgClass(ListReply)

        then:
        result2.jobs.size() == 1
        result2.jobs.find().command == 'Ciao'

        // -- third command - LIST by UUID
        when:
        frontend.tell( new CmdList(jobsId: [ requestId1.toString()[0..8] ]), sender.getRef() )
        def result3 = sender.expectMsgClass(ListReply)
        then:
        result3.jobs.size() == 1
        result3.jobs.find().command == 'Hello'

    }

    def 'text cmd list tasks' () {

        setup:
        final requestId1 = UUID.randomUUID()
        final requestId2 = UUID.randomUUID()

        final task1 = TaskEntry.create(1)  { TaskEntry it -> it.req.requestId = requestId1; it.req.script = 'Hello' }
        final task2 = TaskEntry.create(2)  { TaskEntry it -> it.req.requestId = requestId1; it.req.script = 'Hello'; it.status = TaskStatus.TERMINATED }

        final task3 = TaskEntry.create(3)  { TaskEntry it -> it.req.requestId = requestId2; it.req.script = 'Ciao' }
        final task4 = TaskEntry.create(4)  { TaskEntry it -> it.req.requestId = requestId2; it.req.script = 'Ciao'; it.status = TaskStatus.TERMINATED }
        final task5 = TaskEntry.create(5)  { TaskEntry it -> it.req.requestId = requestId2; it.req.script = 'Ciao'; it.result = new TaskResult(exitCode: 1) }

        dataStore.storeTask(task1)
        dataStore.storeTask(task2)
        dataStore.storeTask(task3)
        dataStore.storeTask(task4)
        dataStore.storeTask(task5)


        /*
         * the jobs
         */
        final job1 = Job.create(requestId1)
        final job2 = Job.create(requestId2) { Job it -> it.status = JobStatus.ERROR }

        dataStore.storeJob(job1)
        dataStore.storeJob(job2)


        /*
         * prepare the command
         */
        def sender = newProbe(test.ActorSpecification.system)
        def frontend = newTestActor(test.ActorSpecification.system,FrontEnd) { new FrontEnd(test.ActorSpecification.dataStore) }

        // -- first command - LIST ALL
        when:
        frontend.tell( new CmdList(tasks: true, all: true), sender.getRef() )
        def result = sender.expectMsgClass(ListReply)

        then:
        result.tasks.size() == 5

        // -- list tasks given the job id
        when:
        frontend.tell( new CmdList(tasks: true, jobsId: [requestId2.toString()]), sender.getRef() )
        def result2 = sender.expectMsgClass(ListReply)

        then:
        result2.tasks.size() == 3

        // list all terminated
        when:
        frontend.tell( new CmdList(tasks: true, status: ['terminated']), sender.getRef() )
        def result3 = sender.expectMsgClass(ListReply)

        then:
        result3.tasks.size() == 2

    }




    def 'test cmd node' () {

        setup:
        def node1 =  NodeDataTest.create(11, 'w1,w2')
        def node2 =  NodeDataTest.create(22, 't0,t1,t2')

        dataStore.storeNode(node1)
        dataStore.storeNode(node2)

        def sender = newProbe(test.ActorSpecification.system)
        def frontend = newTestActor(test.ActorSpecification.system,FrontEnd) { new FrontEnd(test.ActorSpecification.dataStore) }
        def cmd = new CmdNode(ticket: UUID.randomUUID())

        when:
        frontend.tell( cmd, sender.getRef() )
        def response = sender.expectMsgClass(NodeReply)

        then:
        response.nodes.size() == 2
        response.nodes.contains( node1 )
        response.nodes.contains( node1 )

    }




}
