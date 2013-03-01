/*
 * Copyright (c) 2012, the authors.
 *
 *    This file is part of 'Circo'.
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

package circo.model

import org.apache.commons.lang.SerializationUtils
import spock.lang.Specification

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class JobTest extends Specification{

    def 'test equals and hash code' () {

        when:
        def collector1 = new Job(UUID.randomUUID())
        collector1.status = JobStatus.PENDING
        collector1.input = new Context().put('y','a')
        collector1.output = new Context()
        collector1.completionTime = System.currentTimeMillis()
        collector1.numOfTasks = 99

        def copy1 = SerializationUtils.clone(collector1)
        def copy2 = Job.copy(collector1)

        def copy3 = Job.copy(collector1)
        copy3.output.put('x','1')


        then:
        collector1 == copy1
        collector1 == copy2
        collector1 != copy3


    }

    def 'test status flags' () {

        setup:
        def job1 = new Job(UUID.randomUUID())
        def job2 = new Job(UUID.randomUUID())
        def job3 = new Job(UUID.randomUUID())
        def job4 = new Job(UUID.randomUUID())

        when:
        job2.status = JobStatus.PENDING
        job3.status = JobStatus.SUCCESS
        job4.status = JobStatus.ERROR

        then:
        !job1.isSuccess()
        job2.submitted
        job3.success
        job4.failed


    }

}
