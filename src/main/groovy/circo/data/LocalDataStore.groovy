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

package circo.data

import circo.messages.JobEntry
import circo.messages.JobId
import circo.messages.JobStatus
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@CompileStatic
class LocalDataStore extends AbstractDataStore {

    private List jobsListeners = []

    private AtomicLong idGen = new AtomicLong()

    def LocalDataStore() {
        jobsMap = new ConcurrentHashMap<>()
        nodeDataMap = new ConcurrentHashMap<>()
    }

    JobId nextJobId() { new JobId( idGen.addAndGet(1) ) }

    @Override
    protected Lock getLock(key) {
        new ReentrantLock()
    }


    List<JobEntry> findJobsByStatus( JobStatus... status ) {
        assert status
        jobsMap.values().findAll { JobEntry job -> job.status in status  }
    }

    boolean saveJob( JobEntry job ) {
        def isNew = super.saveJob(job)

        if( isNew && jobsListeners )  {
            try {
                jobsListeners.each{ Closure it -> it.call(job) }
            }
            catch( Exception e ) {
                log.error "Failed invoking Add New JobEntry listener", e
            }

        }

        return isNew
    }

    List<JobEntry> findJobsById( final String jobId ) {
        assert jobId

        def value
        if ( jobId.contains('*') ) {
            value = jobId.replace('*','.*')
        }
        else {
            value = jobId
        }

        jobsMap.values().findAll { JobEntry job -> job.id.toHexString() ==~ /$value/ }

    }

    /**
     * Find all jobs with the status specified
     *
     * @param status
     * @return
     */
    @Override
    void addNewJobListener(Closure listener) {
        assert listener
        jobsListeners.add(listener)
    }


    void removeNewJobListener(Closure listener) {
        assert listener
        jobsListeners.remove(listener)
    }

}
