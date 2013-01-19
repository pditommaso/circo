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
import akka.actor.UntypedActor
import akka.dispatch.Futures
import akka.pattern.Patterns
import com.google.common.io.Files
import groovy.util.logging.Slf4j
import rush.data.DataStore
import rush.messages.*
import rush.utils.ProcessHelper

import java.util.concurrent.Callable
/**
 *
 *  @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
class JobExecutor extends UntypedActor {

    /** The reference to the actor monitor the job execution */
    private ActorRef monitor = getContext().actorFor('../mon')

    protected DataStore store

    protected int slow = 0

    def JobExecutor( ) {
    }

    def void preStart() {
        log.debug "++ Starting actor ${getSelf().path()}"
        if( slow ) {
            log.warn "Running in slow mode -- adding ${slow} secs to job execution"
        }
    }

    def void postStop() {
        log.debug "~~ Stopping actor ${getSelf().path()}"
    }

    private Process process

    private int exitCode

    private StringBuilder output

    private boolean cancelRequest

    @Override
    def void onReceive(def message) {
        log.debug "<- $message"

        /*
         * Received a message to execute a new job
         */
        if( message instanceof ProcessToRun ) {
            final job = message.jobEntry

            // check if there's another job running
            if( process ) {
                throw new IllegalStateException("A process is still running: ${process} -- cannot launch job: ${job}")
            }

            // eventually reset the 'cancelRequest' flag
            cancelRequest = false

            // launch the process
            processJobAndNotifyResult(job)

        }

        /*
         * The process exceeded the timeout, destroy it
         */
        else if( message instanceof ProcessKill ) {
            if( process ) {
                log.debug "Destroying process: ${process}"
                // mark if it is 'cancel' job request
                cancelRequest = message.cancel
                // kill the process
                process.destroy()
            }
            else {
                log.warn "Oops! No process to destroy -- message ignored"
            }
        }

        else {
            unhandled(message)
        }
    }


    def void processJobAndNotifyResult( JobEntry job ) {
        assert job

        def callable = new Callable<Object>() {

            @Override
            public Object call() throws Exception {

                exitCode = Integer.MAX_VALUE
                output = new StringBuilder()
                try {
                    new WorkComplete(process(job))
                }
                catch( Throwable err ) {
                    log.error("Unable to process: ${job} -- marked as failed", err)

                    def result = new JobResult(jobId: job?.id, output: output?.toString(), failure: err)
                    new WorkComplete(result)
                }

            }
        }

        // -- notify the job completion to the monitor
        def notifyResult = Futures.future( callable, getContext().dispatcher() )
        Patterns.pipe(notifyResult, getContext().dispatcher()) .to( monitor )

    }

    static File createRushDir( JobEntry job ) {

        File result = new File(job.workDir, '.rush')
        if( !result.exists() && !result.mkdir() ) {
            throw new RuntimeException("Unable to create working directory ${result} -- job ${job} cannot be launched ")
        }

        return result
    }

    static private Random rndgen = new Random()

    static File createWorkDir( int seed ) {

        final baseDir = System.getProperty("java.io.tmpdir")

        long timestamp = System.currentTimeMillis()
        int id = seed
        while( true ) {

            File tempDir = new File(baseDir, "rush-${Integer.toHexString(id)}");
            if (tempDir.mkdir()) {
                return tempDir;
            }

            if( System.currentTimeMillis() - timestamp > 1000 ) {
                throw new IllegalStateException("Unable to create a temporary folder '${tempDir}' -- verify access permissions" )
            }

            Thread.sleep(50)
            id = id + rndgen.nextInt(Integer.MAX_VALUE)
        }


    }


    JobResult process( final JobEntry job ) {
        log.debug "Cmd launching: ${job} "

        def dumper = null
        def result = null
        def scriptFile = null
        def privateDir = null
        try {
            job.launchTime = System.currentTimeMillis()
            job.workDir = createWorkDir( job.id.hashCode() )

            // create the local private dir
            privateDir = createRushDir(job)
            // save the script to a file
            scriptFile = new File(privateDir,'script')
            Files.write( job.req.script.getBytes(), scriptFile )
        }
        finally {
            store.saveJob(job)
        }


        try {

            ProcessBuilder builder = new ProcessBuilder()
                    .directory( job.workDir )
                    .command( job.req.shell, scriptFile.toString() )
                    .redirectErrorStream(true)

            // -- configure the job environment
            if( job.req.environment ) {
                builder.environment().putAll(job.req.environment)
            }
            builder.environment().put('TMPDIR', job.workDir?.absolutePath )

            // -- start the execution and notify the event to the monitor
            process = builder.start()
            job.pid = ProcessHelper.getPid(process)
            job.status = JobStatus.RUNNING
            store.saveJob(job)

            def message = new ProcessStarted(job)
            log.debug "-> ${message}"
            monitor.tell( message )

            // -- consume the produced output
            dumper = new ByteDumper(process.getInputStream(), output, job.req.maxInactive>0 )
            dumper.setName( "dumper-${job}" )
            dumper.start()


            // -- for for the job termination
            if( job.req.maxDuration ) {
                process.waitForOrKill(job.req.maxDuration)
                exitCode = process.exitValue()
            }
            else {
                exitCode = process.waitFor()
            }

            // !!
            // !! Only for test purpose -- add an extra sleep time to simulate slow machine
            // !!
            if( exitCode == 0 && slow ) {
                log.warn "Sleeping $slow secs .. ZzzZzzZzz"
                Thread.sleep(slow * 1000)
            }

            result = new JobResult( jobId: job.id, exitCode: exitCode, output: output.toString(), cancelled: cancelRequest)

        }
        finally {
            // -- save the completion time
            def delta =  System.currentTimeMillis() - job.launchTime
            log.debug "Cmd done: ${job} ==> exit: ${exitCode}; delta: ${delta} ms "

            // -- terminate in any case the byte dumper
            if ( dumper ) {
                dumper.terminate()
            }

            // -- reset also the process variable
            process = null

            if( !result ) {
                new JobResult( jobId: job.id, exitCode: exitCode, output: output.toString(), cancelled: cancelRequest  )
            }

            return result
        }

    }


    private class ByteDumper extends Thread {

        InputStream fInput;
        Appendable fOutput;

        boolean sendAliveMessage

        boolean fTerminated

        public ByteDumper(InputStream input, Appendable output, boolean sendAliveMessage ) {
            this.fInput = new BufferedInputStream(input);
            this.fOutput = output;
            this.sendAliveMessage = sendAliveMessage
        }

        def void terminate() {  fTerminated = true }


        @Override
        public void run() {
            byte[] buf = new byte[8192];
            int next;

            long t1 = System.currentTimeMillis()
            long t2
            while ((next = fInput.read(buf)) != -1 && !fTerminated ) {
                if (fOutput != null) {

                    // -- TODO define the charset on the string constructor
                    fOutput.append( new String(buf,0,next) )

                    // avoid messages overload, do not send if the previous was sent less than a second ago
                    t2 = System.currentTimeMillis()
                    if( sendAliveMessage && t2 - t1 > 1000  ) {
                        monitor.tell( ProcessIsAlive.getInstance() )
                    }
                    t1 = t2
                }
            }

        }

    }


}
