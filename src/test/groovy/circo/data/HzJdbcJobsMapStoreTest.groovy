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

package circo.data
import circo.model.Job
import circo.model.JobStatus
import groovy.sql.Sql
import spock.lang.Shared
import spock.lang.Specification
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class HzJdbcJobsMapStoreTest extends Specification {

    @Shared
    def Sql sql

    /**
     * Set up a H2 in-memory database
     * <p>
     *     H2 SQL reference
     *     http://www.h2database.com/html/grammar.html
     *
     */
    def setup() {

        sql = Sql.newInstance('jdbc:h2:mem:Circo')

        def store = new HzJdbcJobsMapStore(sql: sql)
        store.dropTable()
        store.createTable()
    }


    def 'test store' () {
        setup:
        def jobStore = new HzJdbcJobsMapStore(sql: sql)
        def job1 = new Job(UUID.randomUUID()); job1.status = JobStatus.PENDING; job1.numOfTasks=3
        def job2 = new Job(UUID.randomUUID()); job2.status = JobStatus.PENDING; job2.numOfTasks=99

        when:
        jobStore.store( job1.requestId, job1 )
        jobStore.store( job2.requestId, job2 )

        then:
        jobStore.load(job1.requestId) == job1
        jobStore.load(job2.requestId) == job2
        jobStore.load(job1.requestId) != job2
        jobStore.load(UUID.randomUUID()) == null

        jobStore.loadAll() as Set == [ job1, job2 ] as Set

    }

}
