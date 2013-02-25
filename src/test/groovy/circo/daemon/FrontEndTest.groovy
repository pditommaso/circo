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
import circo.client.CmdStat
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
import circo.reply.StatReply
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


    def 'test cmd job with some id' () {

        setup:
        final task1 = new TaskEntry('1','echo 1')
        final task2 = new TaskEntry('2','echo 2')
        dataStore.storeTask(task1)
        dataStore.storeTask(task2)

        def sender = newProbe(test.ActorSpecification.system)
        def frontend = newTestActor(test.ActorSpecification.system,FrontEnd) { new FrontEnd(test.ActorSpecification.dataStore) }
        def cmd = new CmdStat(jobs: ['1','2', '3'])

        when:
        frontend.tell( cmd, sender.getRef() )
        def result = sender.expectMsgClass(StatReply)

        then:
        result.tasks == [ task1, task2 ]
        result.warn == ["Cannot find any job for id: '3'"]
        result.error.size() == 0
        result.info.size() == 0

    }


    def 'text cmd list' () {

        setup:
        final requestId1 = UUID.randomUUID()
        final requestId2 = UUID.randomUUID()

        final task1 = TaskEntry.create(1)  { TaskEntry it -> it.req.ticket = requestId1; it.req.script = 'Hello' }
        final task2 = TaskEntry.create(2)  { TaskEntry it -> it.req.ticket = requestId1; it.req.script = 'Hello'; it.status = TaskStatus.TERMINATED }

        final task3 = TaskEntry.create(3)  { TaskEntry it -> it.req.ticket = requestId2; it.req.script = 'Ciao' }
        final task4 = TaskEntry.create(4)  { TaskEntry it -> it.req.ticket = requestId2; it.req.script = 'Ciao'; it.status = TaskStatus.TERMINATED }
        final task5 = TaskEntry.create(5)  { TaskEntry it -> it.req.ticket = requestId2; it.req.script = 'Ciao'; it.result = new TaskResult(exitCode: 1) }

        dataStore.storeTask(task1)
        dataStore.storeTask(task2)
        dataStore.storeTask(task3)
        dataStore.storeTask(task4)
        dataStore.storeTask(task5)


        /*
         * the jobs
         */
        final job1 = new Job(requestId1)
        final job2 = new Job(requestId2)
        job2.status = JobStatus.FAILED

        dataStore.storeJob(job1)
        dataStore.storeJob(job2)


        /*
         * prepare the command
         */
        def sender = newProbe(test.ActorSpecification.system)
        def frontend = newTestActor(test.ActorSpecification.system,FrontEnd) { new FrontEnd(test.ActorSpecification.dataStore) }

        def cmd = new CmdList()

        /*
         * WHEN
         */
        when:
        frontend.tell( cmd, sender.getRef() )
        def result = sender.expectMsgClass(ListReply)

        /*
         * THEN
         */
        then:
        result.jobs.size() == 2
        result.jobs.find { it.requestId == requestId1 }.command == 'Hello'
        result.jobs.find { it.requestId == requestId2 }.command == 'Ciao'

        result.jobs.find { it.requestId == requestId1 }.failedTasks == []
        result.jobs.find { it.requestId == requestId1 }.pendingTasks == [task1]

        result.jobs.find { it.requestId == requestId2 }.failedTasks == [task5]
        result.jobs.find { it.requestId == requestId2 }.pendingTasks == [task3, task5]



    }

    def 'test cmd stat' () {

        setup:
        final task1 = new TaskEntry(1,'echo 1')
        final task2 = TaskEntry.create('2')  { it.status = TaskStatus.TERMINATED }
        final task3 = new TaskEntry(3,'echo 3')
        final task4 = TaskEntry.create('4')  { it.status = TaskStatus.TERMINATED }

        test.ActorSpecification.dataStore.storeTask(task1)
        test.ActorSpecification.dataStore.storeTask(task2)
        test.ActorSpecification.dataStore.storeTask(task3)
        test.ActorSpecification.dataStore.storeTask(task4)

        def sender = newProbe(test.ActorSpecification.system)
        def frontend = newTestActor(test.ActorSpecification.system,FrontEnd) { new FrontEnd(test.ActorSpecification.dataStore) }
        def cmd = new CmdStat()
        cmd.status = TaskStatus.TERMINATED.toString()

        when:
        frontend.tell( cmd, sender.getRef() )
        def result = sender.expectMsgClass(StatReply)

        then:
        result.tasks.sort() == [ task2, task4 ]
        result.error.size() == 0
        result.warn.size() == 0
        result.info.size() == 0

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
