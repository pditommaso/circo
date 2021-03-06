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
import java.util.concurrent.TimeUnit

import akka.actor.ActorRef
import akka.actor.Cancellable
import akka.actor.Terminated
import akka.actor.UntypedActor
import circo.messages.ProcessIsAlive
import circo.messages.ProcessKill
import circo.messages.ProcessStarted
import circo.messages.WorkComplete
import groovy.util.logging.Slf4j
import scala.concurrent.duration.FiniteDuration
/**
 *  Monitors the process execution
 *
 *  @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@Mixin(NodeCategory)
class TaskMonitor extends UntypedActor {

    /**
     * The executor actor that launch the process
     */
    private ActorRef executor = getContext().actorFor('../exe')

    private Cancellable watchDog

    private long inactiveTimeout

    /*
     * The current node index. Beside it seems unused, this variable is referenced by the {@code NodeCategory} mixin
     */
    private int nodeId

    /**
     * Initialize the task monitor
     */
    def void preStart() {
        setMDCVariables()
        log.debug "++ Starting actor ${getSelf().path()}"
        getContext().watch(executor)
    }

    /**
     * The the actor stop, clear the current watchdog
     */
    def void postStop() {
        setMDCVariables()
        log.debug "~~ Stopping actor ${getSelf().path()}"
        clearWatchDog()
    }

    @Override
    def void onReceive(def message) {
        setMDCVariables()
        log.debug "<- ${message}"

        /*
         * Process execution has started, so begin to monitor the job
         */
        if( message instanceof ProcessStarted ) {
            final task = message.task

            if( task.req.maxInactive ) {
                inactiveTimeout = task.req.maxInactive
                startWatchDog(inactiveTimeout)
            }
            else {
                this.inactiveTimeout = 0
            }
        }

        /*
         * The executor notify the job is going on
         */
        else if ( message instanceof ProcessIsAlive && inactiveTimeout ) {
            restartWatchDog(inactiveTimeout)
        }


        /*
         * The watchdog timeout has exceeded, destroy the process
         */
        else if ( message instanceof  ProcessKill ) {
            log.debug "-> ${message} to executor actor"

            executor.forward(message, getContext())
            watchDog = null
        }


        /*
         * Work has completed, stop monitoring and notify parent supervisor
         */
        else if( message instanceof WorkComplete ) {
            clearWatchDog()
            log.debug "-> ${message} to parent "
            getContext().parent().forward(message, getContext())
        }

        /*
         * The 'executor' has stopped -- so the 'monitor' stop itself
         */
        else if ( message instanceof Terminated ) {
            getContext().stop(getSelf())
        }

        /*
         * unknown message
         */
        else {
            unhandled(message)
        }

    }

    void startWatchDog( long timeoutInMillis ) {

        FiniteDuration duration = new FiniteDuration( timeoutInMillis, TimeUnit.MILLISECONDS )
        watchDog = getContext().system().scheduler().scheduleOnce(duration, getSelf(), new ProcessKill(), getContext().dispatcher())

    }

    void  restartWatchDog( long timeoutInMillis ) {
        clearWatchDog()
        startWatchDog(timeoutInMillis)
    }

    void clearWatchDog() {
        if ( watchDog ) {
            watchDog.cancel()
            watchDog = null
        }

    }
}
