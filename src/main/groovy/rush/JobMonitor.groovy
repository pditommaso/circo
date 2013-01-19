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
import akka.actor.ActorRef
import akka.actor.Cancellable
import akka.actor.Terminated
import akka.actor.UntypedActor
import groovy.util.logging.Slf4j
import rush.messages.ProcessIsAlive
import rush.messages.ProcessKill
import rush.messages.ProcessStarted
import rush.messages.WorkComplete
import scala.concurrent.duration.FiniteDuration

import java.util.concurrent.TimeUnit

/**
 *  Monitors the process execution
 *
 *  @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
class JobMonitor extends UntypedActor {


    /**
     * The executor actor that launch the process
     */
    private ActorRef executor = getContext().actorFor('../exe')

    private Cancellable watchDog

    private long inactiveTimeout

    def void preStart() {
        log.debug "++ Starting actor ${getSelf().path()}"
        getContext().watch(executor)
    }

    def void postStop() {
        log.debug "~~ Stopping actor ${getSelf().path()}"
        clearWatchDog()
    }

    @Override
    def void onReceive(def message) {
        log.debug "<- ${message}"

        /*
         * Process execution has started, so begin to monitor the job
         */
        if( message instanceof ProcessStarted ) {
            final job = message.jobEntry

            if( job.req.maxInactive ) {
                inactiveTimeout = job.req.maxInactive
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
