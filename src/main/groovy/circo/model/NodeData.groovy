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

package circo.model
import akka.actor.ActorRef
import akka.actor.Address
import circo.util.CircoHelper
import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString
import groovy.util.logging.Slf4j
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */

@Slf4j
@EqualsAndHashCode
@ToString(includes = ['id', 'address','queue','workers','status', 'processed', 'failed'], includePackage = false, includeNames = true)
class NodeData implements Serializable {

    /**
     * The unique node id
     */
    int id

    /**
     * Current status of the node
     */
    NodeStatus status

    /**
     * The node IP address
     */
    Address address

    /**
     * The reference to the master actor for this node
     */
    WorkerRef master

    /*
     * The start timestamp of the node
     */
    long startTimestamp

    /*
     * Number of tasks processed
     */
    long processed

    /*
     * Number of tasks failed
     */
    long failed

    /**
     * Queue of {@code TaskId} waiting to be processed
     */
    Queue<TaskId> queue = new LinkedList<>()

    /**
     * The map of available workers
     */
    Map<WorkerRef, WorkerData> workers = new HashMap<>( Runtime.getRuntime().availableProcessors())

    def NodeData () { }

    def NodeData( NodeData that ) {
        assert that

        this.id = that.id
        this.address = that.address
        this.startTimestamp = that.startTimestamp
        this.processed = that.processed
        this.failed = that.failed

        // make a copy of the queue
        this.queue.addAll( that.queue?: [] )

        // make a deep copy
        this.workers = new HashMap<>(that.workers?.size())
        that.workers.each { WorkerRef ref, WorkerData data ->
            def copy = WorkerData.copy(data)
            this.workers.put( copy.worker, copy )
        }
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

    def boolean assignTaskId( ActorRef actor, TaskId taskId ) {
        this.assignTaskId(new WorkerRef(actor), taskId)
    }

    def boolean assignTaskId( WorkerRef ref, TaskId taskId ) {
        assert ref
        assert taskId

        def info = workers.get(ref)
        if ( !info || info.currentTaskId ) {
            return false
        }

        info.currentTaskId = taskId
        info.processed ++
        this.processed ++

        return true
    }

    def TaskId removeTaskId( WorkerRef ref ) {
        assert ref

        def info = workers.get(ref)
        if ( !info ) {
            return null
        }

        def result = info.currentTaskId
        info.currentTaskId = null
        return result
    }

    def TaskId currentTaskIdFor( WorkerRef ref ) {
        workers.containsKey(ref) ? workers.get(ref).currentTaskId : null
    }


    /**
     * Invoke the specified closure on each entry in the map
     * @param closure
     * @return
     */
    def eachWorker( Closure closure ) {
        workers.values().each{ closure.call(it) }
    }

    def String getStartTimeFmt()  {  CircoHelper.getSmartTimeFormat(startTimestamp) }

    def String toFmtString() {

        // gen info
        def id = CircoHelper.fmt(this.id, 3)
        def addr = CircoHelper.fmt(this.address)
        def stat = this.status?.toString()
        def uptime = this.getStartTimeFmt()

        // workers info
        def procs = CircoHelper.fmt( numOfWorkers(), 2 )
        def runs = CircoHelper.fmt( numOfBusyWorkers(), 2)

        // queue and processed jobs
        def queue = CircoHelper.fmt( numOfQueuedTasks(), 4)
        def count = CircoHelper.fmt( numOfProcessedTasks(), 4 )
        def failed = numOfFailedTasks()

        def jobs = "${queue} /${count}"
        if( failed ) {
            jobs += ' - ' + CircoHelper.fmt(failed, 2)
        }

        "${id} ${addr} ${stat} ${uptime} ${runs} /${procs} $jobs" .toString()

    }

    def int numOfWorkers() {
        workers?.size() ?: 0
    }

    def int numOfBusyWorkers() {
        busyWorkers()?.size()
    }

    def int numOfFreeWorkers() {
       freeWorkers() ?.size()
    }

    def long numOfProcessedTasks() {
        long result = 0
        workers?.values()?.each{ WorkerData wrk -> result += wrk.processed }
        result
    }

    def long numOfFailedTasks() {
        long result = 0
        workers?.values()?.each{ WorkerData wrk -> result += wrk.failed }
        result
    }

    def numOfQueuedTasks() {
        queue?.size() ?: 0
    }


    def List<WorkerData> busyWorkers() {
        workers?.values()?.findAll { WorkerData wrk -> wrk.currentTaskId != null }
    }

    def List<WorkerData> freeWorkers() {
        workers?.values()?.findAll { WorkerData wrk -> wrk.currentTaskId == null }
    }

}
