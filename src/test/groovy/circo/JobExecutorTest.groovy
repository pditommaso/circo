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

import circo.messages.JobEntry
import circo.messages.JobId
import circo.messages.JobReq
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
        def file = JobExecutor.createCircoDir(entry)

        then:
        file.exists()
        file.isDirectory()
        file.name == '.circo'

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
