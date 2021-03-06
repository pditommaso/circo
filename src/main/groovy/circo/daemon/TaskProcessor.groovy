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

package circo.daemon
import static akka.actor.SupervisorStrategy.restart
import static akka.actor.SupervisorStrategy.resume
import static akka.actor.SupervisorStrategy.stop

import akka.actor.ActorRef
import akka.actor.AllForOneStrategy
import akka.actor.Props
import akka.actor.SupervisorStrategy
import akka.actor.SupervisorStrategy.Directive
import akka.actor.UntypedActor
import akka.actor.UntypedActorFactory
import akka.japi.Function
import circo.data.DataStore
import circo.messages.PauseWorker
import circo.messages.ProcessKill
import circo.messages.ProcessToRun
import circo.messages.WorkComplete
import circo.messages.WorkIsDone
import circo.messages.WorkIsReady
import circo.messages.WorkToBeDone
import circo.messages.WorkerCreated
import circo.messages.WorkerFailure
import circo.messages.WorkerRequestsWork
import circo.model.Job
import circo.model.JobStatus
import circo.model.TaskId
import circo.model.TaskResult
import circo.model.TaskStatus
import circo.model.WorkerRef
import groovy.util.logging.Slf4j
import scala.concurrent.duration.Duration
/**
 *
 *  @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@Mixin(NodeCategory)
class TaskProcessor extends UntypedActor {

    enum State { IDLE, RUNNING }

    private State state = State.IDLE

    private int nodeId

    private ActorRef master

    private ActorRef executor

    private ActorRef monitor

    private DataStore store

    private TaskId currentTaskId

    // Only for testing -- add an extra overhead in the job execution to simulation slow machine
    private int slow

    private Map<Class, Closure> idleDispatcher = new HashMap<>()

    private Map<Class,Closure> runningDispatcher = new HashMap<>()


    def TaskProcessor() {

        idleDispatcher[WorkIsReady] = this.&handleWorkIsReady
        idleDispatcher[WorkToBeDone] = this.&handleWorkToBeDone

        runningDispatcher[WorkComplete] = this.&handleWorkComplete
        runningDispatcher[PauseWorker] = this.&handlePauseWorker
        runningDispatcher[ProcessKill] = this.&handleProcessKill

    }

    def errorHandler = { Throwable failure ->

        //-- TODO increment the failures count for the current node
        // when the failures exceed a defined threshold the node is stopped
        // when the failures exceed on all nodes the computation the cluster computation is paused

        log.error( "TaskProcessor Supervisor got an exception", failure )

        if ( currentTaskId ) {
            def result = new TaskResult( taskId: currentTaskId, failure: failure )
            self().tell( new WorkComplete(result), self() )
            return resume()
        }
        else {
            // notify the master of the failure
            log.debug "-> WorkerFailure to $master"
            master.tell( new WorkerFailure(self()), self() )

            return restart();
        }

    }  as Function<Throwable, Directive>


    @Override
    def void preStart() {
        setMDCVariables()
        log.debug "++ Starting actor ${getSelf().path()}"

        // -- create or get refs to required actors
        master = getContext().system().actorFor("/user/${NodeMaster.ACTOR_NAME}")
        executor = getContext().actorOf(new Props({new TaskExecutor(store: store, nodeId: nodeId, slow: slow)} as UntypedActorFactory), 'exe')
        monitor = getContext().actorOf(new Props({new TaskMonitor(nodeId: nodeId)} as UntypedActorFactory), 'mon')

        // notify this actor creation
        master.tell( new WorkerCreated(self()), self() )
    }

    @Override
    def void postStop() {
        setMDCVariables()
        log.debug "~~ Stopping actor ${getSelf().path()}"
    }

    @Override
    def SupervisorStrategy supervisorStrategy () {
        new AllForOneStrategy(1, Duration.create("1 seconds"), errorHandler )
    }


    /**
     *  The main message dispatched method
     */
    @Override
    def void onReceive(def message) {
        setMDCVariables()
        log.debug "<- $message - [state: $state]"

        def table = (state == State.IDLE) ? idleDispatcher : runningDispatcher
        def clazz = message.class
        def handler = table[clazz]
        if ( !handler ) {
            log.warn "Nothing to do for message type: '$clazz.simpleName' in [state: $state]"
            unhandled(message)
            return
        }

        try {
            handler.call(message)
        }
        catch( Throwable e ) {
            log.error("Failure handling message: $message", e)
            stop()
        }

    }


    /**
     * when the master notify that some work is available, the processor
     * reply asking for some work
     */
    protected void handleWorkIsReady(WorkIsReady message) {
        master.tell( new WorkerRequestsWork(self()), self() )
    }

    /**
     * Handle the message {@code WorkToBeDone} send by the master
     * launching the process to be executed through the executor actor
     *
     */
    protected void handleWorkToBeDone( WorkToBeDone message ) {

        final task = store.getTask( message.taskId )
        if( !task ) {
            log.error "Oops! Unknown task with id: ${message.taskId}"
            return
        }

        // -- set the currentJobId
        currentTaskId = task.id

        // -- increment the 'attempts' counter and save it
        task.attemptsCount ++
        task.status = TaskStatus.READY
        task.worker = new WorkerRef(self())
        store.saveTask(task)

        // -- set the job to 'running' status
        final job = store.getJob( task.req.requestId )
        if( job && job.status == JobStatus.PENDING ) {
            store.updateJob(task.req.requestId) { Job thisJob ->
                // change the status to RUNNING
                thisJob.status = JobStatus.RUNNING
            }
        }


        // -- Switching to 'running' mode
        this.state = State.RUNNING

        // -- notify the runner to launch the job
        final processToRun = new ProcessToRun(task)
        log.debug "-> $processToRun to executor"
        executor.tell(processToRun, self())

    }

    /**
     * When the {@code TaskExecutor} actor complete its work, it sends the message {@code WorkComplete}
     *
     * @param message
     */
    protected void handleWorkComplete( WorkComplete message ) {
        final result = message.result as TaskResult

        // -- update the state of the TaskEntry
        final taskId = result.taskId
        final task = store.getTask(taskId)

        // -- check was requested to kill this job
        task.killed = store.removeFromKillList(task.id)

        // -- setting the job result update the job flags as well
        task.result = result
        store.saveTask(task)

        // -- notify the master of the failure
        if( task.isFailed() ) {
            log.debug "Task ${task.id} failed -- ${task.dump()}"
            master.tell( new WorkerFailure(getSelf()), self() )
        }
        else {
            log.debug "Task ${task.id} complete -- ${task.dump()}"
        }

        // -- clear the current jobId
        currentTaskId = null

        // -- notify the work is done
        final worker = new WorkerRef(getSelf())
        log.debug "-> WorkIsDone($worker) to $master"
        master.tell( new WorkIsDone(taskId, worker), self() )

        // -- request more work
        log.debug "-> WorkerRequestsWork(${worker}) to master"
        master.tell( new WorkerRequestsWork(worker), self() )

        // We're idle now
        this.state = State.IDLE
    }

    protected void handlePauseWorker(PauseWorker message) {
        monitor.tell( new ProcessKill(cancel: true), self() )
    }


    protected void handleProcessKill( ProcessKill message ) {
        assert message

        if ( currentTaskId == message.taskId ) {
            monitor.forward( message, context() )
        }
        else {
            log.warn "Oops. Requested to kill task ${message.taskId} but I'm currently processing: ${currentTaskId}"
        }

    }


}
