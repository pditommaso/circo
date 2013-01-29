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

import akka.actor.ActorRef
import akka.actor.Address
import akka.actor.UntypedActor
import akka.dispatch.Futures
import akka.pattern.Patterns
import circo.data.DataStore
import circo.data.FileRef
import circo.exception.MissingInputFileException
import circo.exception.MissingOutputFileException
import circo.messages.*
import circo.util.ProcessHelper
import com.google.common.io.Files
import groovy.io.FileType
import groovy.util.logging.Slf4j

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


    def void preStart() {
        log.debug "++ Starting actor ${getSelf().path()}"
        if( slow ) {
            log.warn "Running in slow mode -- adding ${slow} secs to job execution"
        }
    }

    def void postStop() {
        log.debug "~~ Stopping actor ${getSelf().path()}"
    }

    /** The on-going linux process */
    private Process process

    /** The exit-code as returned by the linux process */
    private int exitCode

    /** The stdout as returned by the linux process */
    private StringBuilder output

    /** Whenever the user has requested to cancel this job */
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

    /**
     * Create a local private directory inside the job context execution directory
     * @param job
     * @return
     */
    static File createPrivateDir( JobEntry job ) {

        File result = new File(job.workDir, '.circo')
        if( !result.exists() && !result.mkdir() ) {
            throw new RuntimeException("Unable to create working directory ${result} -- job ${job} cannot be launched ")
        }

        return result
    }

    static private Random rndGen = new Random()

    /**
     * The process scratch folder
     * @param seed
     * @return
     */
    static File createScratchDir( int seed ) {

        final baseDir = System.getProperty("java.io.tmpdir")

        long timestamp = System.currentTimeMillis()
        int id = seed
        while( true ) {

            File tempDir = new File(baseDir, "circo-${Integer.toHexString(id)}");
            if (tempDir.mkdir()) {
                return tempDir;
            }

            if( System.currentTimeMillis() - timestamp > 1000 ) {
                throw new IllegalStateException("Unable to create a temporary folder '${tempDir}' -- verify access permissions" )
            }

            Thread.sleep(50)
            id = id + rndGen.nextInt(Integer.MAX_VALUE)
        }
    }

    /**
     * The main job execution method
     *
     * @param job The job to be executed
     * @return The result of the execution
     */
    JobResult process( final JobEntry job ) {
        log.debug "Cmd launching: ${job} "

        def dumper = null
        def result = null
        def scriptFile = null
        def privateDir = null

        def scriptToExecute = stage(job.req)

        /*
         * create the private scratch directory to run the task
         */
        try {
            job.launchTime = System.currentTimeMillis()
            job.workDir = createScratchDir( job.id.hashCode() )

            // create the local private dir
            privateDir = createPrivateDir(job)
            // save the script to a file
            scriptFile = new File(privateDir,'script')
            Files.write( scriptToExecute.getBytes(), scriptFile )
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

            // collect the files produced by this job
            gather(job.req, result, job.workDir, monitor.path().address())
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
                result = new JobResult( jobId: job.id, exitCode: exitCode, output: output.toString(), cancelled: cancelRequest  )
            }

            return result
        }

    }

    def static BASIC_VARIABLE = /\$([^ \n\r\f\t\$]+)/

    static private String stage( JobReq request ) {
        assert request

        if ( !request.script ) { // nothing to do
            return request.script
        }

        def missing = []


        def result = request.script.replaceAll(BASIC_VARIABLE) {

            // the matched variable e.g. $some_file
            String original = it[0]
            // the token e.g 'some_file'
            String token = it[1]

            // make sure that the keyword exists in the current context
            if ( !request.context.contains(token) ) {
                return original
            }

            // Replace the 'token' with the associated value in the context
            // A token associated to a collection is joined by separating elements by a blank
            def value = request.getContext().getData(token)
            if( value instanceof Collection ) {
                return value.join(' ')
            }

            return value?.toString()
        }


        if ( missing ) { throw new MissingInputFileException(missing) }

        /*
         * +++ TODO +++ copy remote files in the local folder
         */

        return result.toString()
    }

    /**
     * fetch the expected produced files on the fle system
     *
     * @param job
     * @param result
     */
    static private JobResult gather(JobReq req, JobResult result, File workDir, Address nodeAddress ) {
        assert req
        assert result
        assert workDir

        /*
         * create the result context object
         */
        JobContext deltaContext = new JobContext()

        /*
         * scan for the produced files
         */
        List<File> missing = []
        req?.produce?.each { String it ->

            String name = null
            String filePattern
            def p = it.indexOf(':')
            if( p == -1 ) {
                filePattern = it
            }
            else {
                name = it.substring(0,p)
                filePattern = it.substring(p+1)
            }

            // replace any wildcards characters
            // TODO give a try to http://code.google.com/p/wildcard/  -or- http://commons.apache.org/io/
            filePattern = filePattern.replace("?", ".?").replace("*", ".*?")

            // scan to find the file with that name
            int count=0
            workDir.eachFileMatch(FileType.FILES, ~/$filePattern/ ) { File file ->
                deltaContext.add( new FileRef(name?:file.name, file, nodeAddress) )
                count++
            }

            // cannot find any file with that name, return an error
            if ( count == 0 ) { missing << name }
        }

        if ( missing ) {
            throw new MissingOutputFileException(missing as File[])
        }

        result.context = deltaContext
        return result
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
