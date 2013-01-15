/*
 * Copyright (c) 2012, the authors.
 *
 *    This file is part of Rush.
 *
 *    Rush is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation, either version 3 of the License, or
 *    (at your option) any later version.
 *
 *    Rush is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with Rush.  If not, see <http://www.gnu.org/licenses/>.
 */

package rush.frontend
import rush.client.cmd.CmdStat
import rush.client.cmd.CmdSub
import rush.messages.JobEntry
import rush.messages.JobId
import rush.messages.JobStatus
import scala.concurrent.duration.Duration
import test.ActorSpecification

import static test.TestHelper.newProbe
import static test.TestHelper.newTestActor
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class FrontEndTest extends ActorSpecification {

    def 'test CmdSub' () {

        setup:
        def sender = newProbe(system)
        def master = newProbe(system)
        def frontend = newTestActor(system,FrontEnd) {
            def FE = new FrontEnd(dataStore)
            FE.master = master.getRef()
            return FE
        }

        final ID = JobId.of(UUID.randomUUID())
        final sub = new CmdSub()
        sub.ticket = ID.ticket
        sub.command = ['echo', 'Hello world']
        sub.env.put('VAR1', 'val_1')
        sub.env.put('VAR2', 'val_2')
        sub.maxDuration = Duration.create('1 min')
        sub.maxAttempts = 4

        def listenerEntry = null
        dataStore.addNewJobListener { it -> listenerEntry = it }


        when:
        frontend.tell(sub, sender.getRef())
        def result = sender.expectMsgClass(CmdSubResponse)
        def entry = dataStore.getJob(ID)



        then:
        result.success
        result.ticket == sub.ticket
        result.messages.size() == 0

        entry.creationTime != null
        entry.req.maxAttempts == sub.maxAttempts
        entry.req.maxDuration == sub.maxDuration.toMillis()
        entry.req.environment == sub.env
        entry.req.script == sub.command.join(' ')

        listenerEntry == entry


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
        def result = sender.expectMsgClass(CmdStatResponse)

        then:
        result.success
        result.jobs == [ job1, job2 ]
        result.warn == ["Cannot find any job for id: '3'"]
        result.error.size() == 0
        result.info.size() == 0

    }

    def 'test CmdJob By Status' () {

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
        def result = sender.expectMsgClass(CmdStatResponse)

        then:
        result.success
        result.jobs.sort() == [ job2, job4 ]
        result.error.size() == 0
        result.warn.size() == 0
        result.info.size() == 0

    }




}
