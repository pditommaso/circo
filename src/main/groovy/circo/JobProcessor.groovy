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
import akka.actor.*
import akka.actor.SupervisorStrategy.Directive
import akka.japi.Function
import akka.japi.Procedure
import groovy.util.logging.Slf4j
import circo.data.DataStore
import circo.data.WorkerRef
import circo.messages.*
import scala.concurrent.duration.Duration

import static akka.actor.SupervisorStrategy.restart
import static akka.actor.SupervisorStrategy.resume
/**
 *
 *  @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
class JobProcessor extends UntypedActor {

    private ActorRef master

    private ActorRef executor

    private ActorRef monitor

    private DataStore store

    private JobId currentJobId

    // Only for testing -- add an extra overhead in the job execution to simulation slow machine
    private int slow

    def errorHandler = { Throwable failure ->

        //-- TODO increment the failures count for the current node
        // when the failures exceed a defined threshold the node is stopped
        // when the failures exceed on all nodes the computation the cluster computation is paused

        log.error( "Unknown failure", failure )

        if ( currentJobId ) {
            def result = new JobResult( jobId: currentJobId, failure: failure )
            getSelf().tell new WorkComplete(result)
            return resume()
        }
        else {
            // notify the master of the failure
            log.debug "-> WorkerFailure to $master"
            master.tell( new WorkerFailure(getSelf()) )

            return restart();
        }

    }  as Function<Throwable, Directive>


    @Override
    def void preStart() {
        log.debug "++ Starting actor ${getSelf().path()}"

        // -- create or get refs to required actors
        master = getContext().system().actorFor("/user/${JobMaster.ACTOR_NAME}")
        executor = getContext().actorOf(new Props({new JobExecutor(store: store, slow: slow)} as UntypedActorFactory), 'exe')
        monitor = getContext().actorOf(new Props(JobMonitor), 'mon')

        // notify this actor creation
        master.tell( new WorkerCreated(getSelf()) )
    }

    @Override
    def void postStop() {
        log.debug "~~ Stopping actor ${getSelf().path()}"
    }

    @Override
    def SupervisorStrategy supervisorStrategy () {
        new AllForOneStrategy(1, Duration.create("1 seconds"), errorHandler )
    }

    def idle = new Procedure<Object>() {

        @Override
        def void apply(def message ) {

            // Master says there's work to be done, let's ask for it
            if( message instanceof WorkIsReady  ) {
                log.debug("[idle] <- WorkIsReady()")

                message = new WorkerRequestsWork(getSelf())
                log.debug("[idle] -> ${message}")
                master.tell message
            }

            // Send the work off to the implementation
            else if( message instanceof WorkToBeDone ) {
                log.debug("[idle] <- ${message}")

                final entry = store.getJob( message.jobId )
                if( !entry ) {
                    log.error "Oops! Unknown job-id: ${message.jobId}"
                    return
                }

                // -- set the currentJobId
                currentJobId = entry.id

                // -- increment the 'attempts' counter and save it
                entry.attempts ++
                entry.status = JobStatus.READY
                entry.worker = new WorkerRef(getSelf())
                store.saveJob(entry)

                // -- Switching to 'working' mode
                log.debug "[idle] => [working]"
                getContext().become(working)

                // -- notify the runner to launch the job
                final processToRun = new ProcessToRun(entry)
                log.debug "-> $message to executor"
                executor.tell(processToRun)

            }


        }
    }


    def Procedure<Object> working = new Procedure<Object>() {

        @Override
        void apply(def message) {

            // Pass... we're already working
            if( message instanceof WorkIsReady ) {
                log.debug("[working] <- ${message} -- ignore")
            }

            // Pass... we shouldn't even get it
            else if( message instanceof WorkToBeDone ) {
                log.error("[working] <- ${message} -- Master told me to do work, while I'm working.")
            }

            else if( message instanceof WorkComplete ) {
                log.debug("[working] <- ${message} -- Work is complete")

                final result = message.result as JobResult

                // -- update the state of the JobEntry
                final jobId = result.jobId
                final worker = new WorkerRef(getSelf())
                final job = store.getJob(jobId)
                job.worker = worker
                // -- setting the job result update the job flags as well
                job.result = result
                store.saveJob(job)

                // -- notify the master of the failure
                if( job.failed ) {
                    log.debug "-> WorkerFailure to $master"
                    master.tell( new WorkerFailure(getSelf()) )
                }

                // -- clear the current jobId
                currentJobId = null

                // -- notify the work is done
                log.debug "-> WorkIsDone($worker) to $master"
                master.tell( new WorkIsDone(worker, job) )

                // -- request more work
                log.debug "-> WorkerRequestsWork($worker) to $master"
                master.tell new WorkerRequestsWork(worker)


                // We're idle now
                log.debug("[working] => [idle]")
                getContext().become(idle)
            }

            /*
             * kill to current job
             */
            else if( message instanceof PauseWorker ) {
                monitor.tell( new ProcessKill(cancel: true), getSelf() )
            }

        }
    }


    @Override
    def void onReceive(def message) {
        idle.apply(message)
    }




}
