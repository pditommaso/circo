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

package rush

import rush.messages.JobEntry
import rush.messages.JobId
import rush.messages.JobReq
import spock.lang.Specification

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class JobExecutorTest extends Specification {


    def "test createRushDir" () {

        when:
        def entry = new JobEntry(JobId.of(123), new JobReq(script: 'echo Hello world!'))
        entry.workDir = new File(System.getProperty('java.io.tmpdir'))
        def file = JobExecutor.createRushDir(entry)

        then:
        file.exists()
        file.isDirectory()
        file.name == '.rush'

        cleanup:
        file.delete()

    }


    def "test createWorkDir" () {

        when:
        def path = JobExecutor.createWorkDir(123554)
        println path

        then:
        path.exists()
        path.isDirectory()

        cleanup:
        path.delete()
    }



}
