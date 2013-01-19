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

package circo.client.cmd

import circo.data.WorkerRefMock
import circo.messages.JobEntry
import circo.messages.JobStatus
import spock.lang.Specification
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class CmdStatTest extends Specification  {

    def "test printJobs" () {

        when:

        def list = []
        list << JobEntry.create('1') { it.status = JobStatus.NEW }
        list << JobEntry.create('2') { it.status = JobStatus.PENDING }
        list << JobEntry.create('88') {
            it.status = JobStatus.RUNNING;
            it.launchTime = (new Date()+1).time;
            it.worker = new WorkerRefMock('/a/b/c')
        }

        CmdStat.printJobsTable(list)

        then:
        noExceptionThrown()

    }


    def "test printNoJobs" () {

        when:
        CmdStat.printJobsTable([])

        then:
        noExceptionThrown()
    }



//    def "test job with status"() {
//
//        when:
//        def cmd = CommandParser
//
//        then:
//
//    }

}
