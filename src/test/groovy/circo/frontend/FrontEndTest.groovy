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

package circo.frontend
import circo.client.CmdNode
import circo.client.CmdStat
import circo.client.CmdSub
import circo.model.NodeDataTest
import circo.model.TaskContext
import circo.model.TaskEntry
import circo.model.TaskId
import circo.model.TaskStatus
import circo.reply.NodeReply
import circo.reply.StatReply
import circo.reply.SubReply
import scala.concurrent.duration.Duration
import test.ActorSpecification

import static test.TestHelper.newProbe
import static test.TestHelper.newTestActor
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class FrontEndTest extends ActorSpecification {

    def 'test cmd sub' () {

        setup:
        def sender = newProbe(system)
        def master = newProbe(system)
        def frontend = newTestActor(system,FrontEnd) {
            def FE = new FrontEnd(dataStore)
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

        def listenerEntry = null
        dataStore.addNewTaskListener { it -> listenerEntry = it }


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
        def entry = dataStore.getTask( ID )

        then:
        result.taskIds.size() == 1
        result.ticket == sub.ticket
        result.messages.size() == 0

        entry.creationTime != null
        entry.req.maxAttempts == sub.maxAttempts
        entry.req.maxDuration == sub.maxDuration.toMillis()
        entry.req.environment.each{ k, v -> sub.env.get(k) == v }
        entry.req.environment['JOB_ID'] == ID.toFmtString()
        //entry.req.environment == sub.env
        entry.req.script == sub.command.join(' ')

        listenerEntry == entry

    }


    def 'test cmd sub --each' () {

        setup:
        def sender = newProbe(system)
        def master = newProbe(system)
        def frontend = newTestActor(system,FrontEnd) {
            def FE = new FrontEnd(dataStore)
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
        sub.context = new TaskContext().put('X','1..2').put('Y','[alpha,beta]')

        // submit for each values in the (X,Y) pair, so there ae 4 combinations
        sub.eachItems = ['X','Y']

        when:
        frontend.tell(sub, sender.getRef())

        // the a new job entry has been created
        def result = sender.expectMsgClass(SubReply)
        def entry0 = dataStore.getTask( result.taskIds[0] )
        def entry1 = dataStore.getTask( result.taskIds[1] )
        def entry2 = dataStore.getTask( result.taskIds[2] )
        def entry3 = dataStore.getTask( result.taskIds[3] )

        then:
        result.taskIds.size() == 4
        result.taskIds[0] == TaskId.of(1)
        entry0.req.environment['JOB_ID'] == entry0.id.toFmtString()
        entry0.req.context.getData('X') == '1'
        entry0.req.context.getData('Y') == 'alpha'

        entry1.req.environment['JOB_ID'] == entry1.id.toFmtString()
        entry1.req.context.getData('X') == '2'
        entry1.req.context.getData('Y') == 'alpha'

        entry2.req.environment['JOB_ID'] == entry2.id.toFmtString()
        entry2.req.context.getData('X') == '1'
        entry2.req.context.getData('Y') == 'beta'

        entry3.req.environment['JOB_ID'] == entry3.id.toFmtString()
        entry3.req.context.getData('X') == '2'
        entry3.req.context.getData('Y') == 'beta'

    }


    def 'test cmd job with some id' () {

        setup:
        final job1 = new TaskEntry('1','echo 1')
        final job2 = new TaskEntry('2','echo 2')
        dataStore.saveTask(job1)
        dataStore.saveTask(job2)

        def sender = newProbe(system)
        def frontend = newTestActor(system,FrontEnd) { new FrontEnd(dataStore) }
        def cmd = new CmdStat(jobs: ['1','2', '3'])

        when:
        frontend.tell( cmd, sender.getRef() )
        def result = sender.expectMsgClass(StatReply)

        then:
        result.tasks == [ job1, job2 ]
        result.warn == ["Cannot find any job for id: '3'"]
        result.error.size() == 0
        result.info.size() == 0

    }

    def 'test cmd stat' () {

        setup:
        final job1 = new TaskEntry(1,'echo 1')
        final job2 = TaskEntry.create('2')  { it.status = TaskStatus.TERMINATED }
        final job3 = new TaskEntry(3,'echo 3')
        final job4 = TaskEntry.create('4')  { it.status = TaskStatus.TERMINATED }

        dataStore.saveTask(job1)
        dataStore.saveTask(job2)
        dataStore.saveTask(job3)
        dataStore.saveTask(job4)

        def sender = newProbe(system)
        def frontend = newTestActor(system,FrontEnd) { new FrontEnd(dataStore) }
        def cmd = new CmdStat()
        cmd.status = [ TaskStatus.TERMINATED ]

        when:
        frontend.tell( cmd, sender.getRef() )
        def result = sender.expectMsgClass(StatReply)

        then:
        result.tasks.sort() == [ job2, job4 ]
        result.error.size() == 0
        result.warn.size() == 0
        result.info.size() == 0

    }


    def 'test cmd node' () {

        setup:
        def node1 =  NodeDataTest.create(11, 'w1,w2')
        def node2 =  NodeDataTest.create(22, 't0,t1,t2')

        dataStore.putNodeData(node1)
        dataStore.putNodeData(node2)

        def sender = newProbe(system)
        def frontend = newTestActor(system,FrontEnd) { new FrontEnd(dataStore) }
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
