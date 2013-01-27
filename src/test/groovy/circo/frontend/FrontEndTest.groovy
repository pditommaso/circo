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
import circo.client.cmd.CmdNode
import circo.client.cmd.CmdStat
import circo.client.cmd.CmdSub
import circo.data.NodeDataTest
import circo.messages.JobEntry
import circo.messages.JobId
import circo.messages.JobStatus
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
        dataStore.addNewJobListener { it -> listenerEntry = it }


        when:
        frontend.tell(sub, sender.getRef())

        /*
         * this should make part of the 'expected' results
         */

        // the sender get a 'reply' message
        def result = sender.expectMsgClass(SubReply)
        // the reply contains the ID(s) of the new submitted job(s)
        def ID = result.jobIds.get(0)
        // the a new job entry has been created
        def entry = dataStore.getJob( ID )

        then:
        result.jobIds.size() == 1
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


    /*
     * TODO ++++ to be fixed
     */
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
        sub.eachItems = ['alpha','beta','delta']

        when:
        frontend.tell(sub, sender.getRef())
        // the a new job entry has been created
        def result = sender.expectMsgClass(SubReply)
        def entry0 = dataStore.getJob( result.jobIds[0] )
        def entry1 = dataStore.getJob( result.jobIds[1] )
        def entry2 = dataStore.getJob( result.jobIds[2] )

        then:
        result.jobIds.size() == 3
        result.jobIds[0] == JobId.of(1)
        entry0.req.environment['JOB_ID'] == entry0.id.toFmtString()
        entry0.req.environment['JOB_INDEX'] == 'alpha'

        entry1.req.environment['JOB_ID'] == entry1.id.toFmtString()
        entry1.req.environment['JOB_INDEX'] == 'beta'

        entry2.req.environment['JOB_ID'] == entry2.id.toFmtString()
        entry2.req.environment['JOB_INDEX'] == 'delta'

    }


    def 'test cmd job with some id' () {

        setup:
        final job1 = new JobEntry('1','echo 1')
        final job2 = new JobEntry('2','echo 2')
        dataStore.saveJob(job1)
        dataStore.saveJob(job2)

        def sender = newProbe(system)
        def frontend = newTestActor(system,FrontEnd) { new FrontEnd(dataStore) }
        def cmd = new CmdStat(jobs: ['1','2', '3'])

        when:
        frontend.tell( cmd, sender.getRef() )
        def result = sender.expectMsgClass(StatReply)

        then:
        result.jobs == [ job1, job2 ]
        result.warn == ["Cannot find any job for id: '3'"]
        result.error.size() == 0
        result.info.size() == 0

    }

    def 'test cmd stat' () {

        setup:
        final job1 = new JobEntry(1,'echo 1')
        final job2 = JobEntry.create('2')  { it.status = JobStatus.COMPLETE }
        final job3 = new JobEntry(3,'echo 3')
        final job4 = JobEntry.create('4')  { it.status = JobStatus.COMPLETE }

        dataStore.saveJob(job1)
        dataStore.saveJob(job2)
        dataStore.saveJob(job3)
        dataStore.saveJob(job4)

        def sender = newProbe(system)
        def frontend = newTestActor(system,FrontEnd) { new FrontEnd(dataStore) }
        def cmd = new CmdStat()
        cmd.status = [ JobStatus.COMPLETE ]

        when:
        frontend.tell( cmd, sender.getRef() )
        def result = sender.expectMsgClass(StatReply)

        then:
        result.jobs.sort() == [ job2, job4 ]
        result.error.size() == 0
        result.warn.size() == 0
        result.info.size() == 0

    }


    def 'test cmd node' () {

        setup:
        def node1 =  NodeDataTest.create('1.1.1.1:2551', 'w1,w2')
        def node2 =  NodeDataTest.create('1.1.2.2:2555', 't0,t1,t2')

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
