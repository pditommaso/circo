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

import akka.actor.ActorRef
import akka.actor.Address
import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString
import groovy.util.logging.Slf4j
import circo.messages.JobId
import circo.utils.RushHelper
import circo.utils.SerialVer

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */

@Slf4j
@SerialVer
@EqualsAndHashCode
@ToString(includes = ['address','queue','workers','status'], includePackage = false)
class NodeData implements Serializable {

    NodeStatus status

    Address address

    long startTimestamp

    long processed

    long failed

    Queue<JobId> queue = new LinkedList<>()

    Map<WorkerRef, WorkerData> workers = new HashMap<>( Runtime.getRuntime().availableProcessors())

    def NodeData () { }

    def NodeData( NodeData that ) {
        assert that
        this.address = that.address
        this.startTimestamp = that.startTimestamp
        this.processed = that.processed
        this.failed = that.failed
        this.workers = new HashMap<>(that.workers)
    }

    def void failureInc( WorkerRef ref ) {
        assert ref

        def data = getWorkerData(ref)

        if( data ) {
             // -- increment the worker failure counter
            data.failed++
            // -- increment the node failure counter
            failed ++
        }
    }

    def WorkerData createWorkerData( WorkerRef workerRef ) {
        def data = new WorkerData(workerRef)
        putWorkerData(data)
        return data
    }

    def WorkerData createWorkerData( ActorRef actor ) {
        createWorkerData(new WorkerRef(actor))
    }

    /**
     * Initialize the map entry for the specified worker
     */
    def WorkerData putWorkerData( WorkerData data ) {
        workers.put( data.worker, data )
    }

    def boolean hasWorkerData( WorkerRef worker ) {
        workers.containsKey(worker)
    }

    def WorkerData getWorkerData( WorkerRef worker ) {
        workers.get(worker)
    }

    def WorkerData removeWorkerData( WorkerRef worker ) {
        assert worker

        if ( this.workers.containsKey( worker ) ) {
            return workers.get(worker)
        }
        else {
            return null
        }

    }

    def boolean assignJobId( ActorRef actor, JobId jobId ) {
        this.assignJobId(new WorkerRef(actor), jobId)
    }

    def boolean assignJobId( WorkerRef ref, JobId jobId ) {
        assert ref
        assert jobId

        def info = workers.get(ref)
        if ( !info || info.currentJobId ) {
            return false
        }

        info.currentJobId = jobId
        info.processed ++
        this.processed ++

        return true
    }

    def JobId removeJobId( WorkerRef ref ) {
        assert ref

        def info = workers.get(ref)
        if ( !info ) {
            return null
        }

        def result = info.currentJobId
        info.currentJobId = null
        return result
    }


    /**
     * Invoke the specified closure on each entry in the map
     * @param closure
     * @return
     */
    def eachWorker( Closure closure ) {
        workers.values().each{ closure.call(it) }
    }

    def void clearWorkers() {
        workers.clear()
    }

    def String getStartTimeFmt()  {  RushHelper.getSmartTimeFormat(startTimestamp) }

    def String toFmtString() {

        // gen info
        def addr = RushHelper.fmt(this.address)
        def stat = this.status?.toString()
        def uptime = this.getStartTimeFmt()

        // workers info
        def procs = RushHelper.fmt( numOfWorkers(), 2 )
        def runs = RushHelper.fmt( numOfBusyWorkers(), 2)

        // queue and processed jobs
        def queue = RushHelper.fmt( numOfQueuedJobs(), 4)
        def count = RushHelper.fmt( numOfProcessedJobs(), 4 )
        def failed = numOfFailedJobs()

        def jobs = "${queue} /${count}"
        if( failed ) {
            jobs += ' - ' + RushHelper.fmt(failed, 2)
        }

        "${addr} ${stat} ${uptime} ${runs} /${procs} $jobs" .toString()

    }

    def int numOfWorkers() {
        workers?.size() ?: 0
    }

    def int numOfBusyWorkers() {
        workers?.values()?.findAll { WorkerData wrk -> wrk.currentJobId != null } ?.size()
    }

    def int numOfAvailWorkers() {
        workers?.values()?.findAll { WorkerData wrk -> wrk.currentJobId == null } ?.size()
    }

    def long numOfProcessedJobs() {
        long result = 0
        workers?.values()?.each{ WorkerData wrk -> result += wrk.processed }
        result
    }

    def long numOfFailedJobs() {
        long result = 0
        workers?.values()?.each{ WorkerData wrk -> result += wrk.failed }
        result
    }

    def numOfQueuedJobs() {
        queue?.size() ?: 0
    }
}
