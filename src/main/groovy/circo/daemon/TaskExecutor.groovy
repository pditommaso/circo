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
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import akka.actor.ActorRef
import akka.actor.UntypedActor
import akka.dispatch.Futures
import akka.pattern.Patterns
import circo.data.DataStore
import circo.exception.MissingInputFileException
import circo.exception.MissingOutputFileException
import circo.messages.ProcessIsAlive
import circo.messages.ProcessKill
import circo.messages.ProcessStarted
import circo.messages.ProcessToRun
import circo.messages.WorkComplete
import circo.model.Context
import circo.model.FileRef
import circo.model.TaskEntry
import circo.model.TaskReq
import circo.model.TaskResult
import circo.model.TaskStatus
import circo.util.CircoHelper
import circo.util.ProcessHelper
import com.google.common.io.Files
import groovy.io.FileType
import groovy.util.logging.Slf4j
/**
 *
 *  @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@Mixin(NodeCategory)
class TaskExecutor extends UntypedActor {

    /** The reference to the actor monitor the job execution */
    private ActorRef monitor = getContext().actorFor('../mon')

    protected DataStore store

    protected int slow = 0

    protected int nodeId

    def void preStart() {
        setMDCVariables()

        log.debug "++ Starting actor ${getSelf().path()}"
        if( slow ) {
            log.warn "Running in slow mode -- adding ${slow} secs to job execution"
        }
    }

    def void postStop() {
        setMDCVariables()
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
        setMDCVariables()
        log.debug "<- $message"

        /*
         * Received a message to execute a new job
         */
        if( message instanceof ProcessToRun ) {
            final task = message.task

            // check if there's another job running
            if( process ) {
                throw new IllegalStateException("A process is still running: ${process} -- cannot launch job: ${task}")
            }

            // eventually reset the 'cancelRequest' flag
            cancelRequest = false

            // launch the process
            processJobAndNotifyResult(task)

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


    def void processJobAndNotifyResult( TaskEntry task ) {
        assert task

        def callable = new Callable<Object>() {

            @Override
            public Object call() throws Exception {

                exitCode = Integer.MAX_VALUE
                output = new StringBuilder()
                try {
                    new WorkComplete(process(task))
                }
                catch( Throwable err ) {
                    log.error("Unable to process: ${task} -- marked as failed", err)

                    def result = new TaskResult(taskId: task?.id, output: output?.toString(), failure: err)
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
     * @param task
     * @return
     */
    static File createPrivateDir( TaskEntry task ) {

        File result = new File(task.workDir, '.circo')
        if( !result.exists() && !result.mkdir() ) {
            throw new RuntimeException("Unable to create working directory ${result} -- job ${task} cannot be launched ")
        }

        return result
    }



    /**
     * The main job execution method
     *
     * @param task The job to be executed
     * @return The result of the execution
     */
    TaskResult process( final TaskEntry task ) {
        log.debug "Cmd launching: ${task} "

        def dumper = null
        def result = null
        def scriptFile = null
        def privateDir = null

        def scriptToExecute = stage(task.req)

        /*
         * create the private scratch directory to run the task
         */
        task.launchTime = System.currentTimeMillis()
        task.workDir = CircoHelper.createScratchDir()

        // create the local private dir
        privateDir = createPrivateDir(task)
        // save the script to a file
        scriptFile = new File(privateDir,'script')
        Files.write( scriptToExecute.getBytes(), scriptFile )


        try {

            ProcessBuilder builder = new ProcessBuilder()
                    .directory( task.workDir )
                    .command( task.req.shell, scriptFile.toString() )
                    .redirectErrorStream(true)

            // -- configure the job environment
            if( task.req.environment ) {
                builder.environment().putAll(task.req.environment)
            }
            builder.environment().put('TMPDIR', task.workDir?.absolutePath )

            // -- start the execution and notify the event to the monitor
            process = builder.start()
            task.pid = ProcessHelper.getPid(process)
            task.status = TaskStatus.RUNNING
            store.saveTask(task);

            def message = new ProcessStarted(task)
            log.debug "-> ${message}"
            monitor.tell( message, self() )

            // -- consume the produced output
            dumper = new ByteDumper(process.getInputStream(), output, task.req.maxInactive>0 )
            dumper.setName( "dumper-${task}" )
            dumper.start()


            // -- for for the job termination
            if( task.req.maxDuration ) {
                process.waitForOrKill(task.req.maxDuration)
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

            // --
            // await the dumper thread terminates to copy the stdout stream
            // this is necessary when the executed process is very fast, and terminate before the
            // dumper thread  is able to copy all the produced output
            dumper.await(1000)
            dumper = null

            result = new TaskResult( taskId: task.id, exitCode: exitCode, output: output.toString(), cancelled: cancelRequest)

            // collect the files produced by this job
            gather(task.req, result, task.workDir)
        }
        finally {
            // -- save the completion time
            def delta =  System.currentTimeMillis() - task.launchTime
            log.debug "Cmd done: ${task} ==> exit: ${exitCode}; delta: ${delta} ms "

            // -- terminate in any case the byte dumper
            if ( dumper ) {
                dumper.terminate()
            }

            // -- reset also the process variable
            process = null

            if( !result ) {
                result = new TaskResult( taskId: task.id, exitCode: exitCode, output: output.toString(), cancelled: cancelRequest  )
            }

            return result
        }

    }

    def static BASIC_VARIABLE = /\$([^ \n\r\f\t\$]+)/

    /**
     * Invokes the list action, wrapping them with a transaction if they are more than one
     * @param actions
     */
    private void safeApply( Closure... actions ) {

        if( !actions ) return

        if( actions.size() == 1 ) {
            actions[0].call()
        }

        else {
            store.withTransaction {
                actions.each { it.call() }
            }
        }

    }

    static private String stage( TaskReq request ) {
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
    static protected TaskResult gather(TaskReq req, TaskResult result, File workDir) {
        assert req
        assert result
        assert workDir

        /*
         * create the result context object
         */
        Context deltaContext = new Context()

        /*
         * scan for the produced files
         */
        List<File> missing = []
        req?.produce?.each { String it ->

            String name = null
            String filePattern
            def p = it.indexOf('=')
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
                try {
                    deltaContext.add( new FileRef(file, name ?: file.name) )
                    count++
                }
                catch( Exception e ) {
                    log.error("Cannot add file: $file", e )
                }
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

        CountDownLatch barrier = new CountDownLatch(1)

        public ByteDumper(InputStream input, Appendable output, boolean sendAliveMessage ) {
            this.fInput = new BufferedInputStream(input);
            this.fOutput = output;
            this.sendAliveMessage = sendAliveMessage
        }

        /**
         *  Interrupt the dumper thread
         */
        def void terminate() { fTerminated = true }

        /**
         * Await that the thread finished to read the process stdout
         *
         * @param millis Maximum time (in millis) to await
         */
        def void await(long millis=0) {
            if( millis ) {
                barrier.await(millis, TimeUnit.MILLISECONDS)
            }
            else {
                barrier.await()

            }
        }


        @Override
        public void run() {

            try {
                consume()
            }
            finally{
                barrier.countDown()
            }

        }

        /**
         * Consume the process stdout
         */
        protected void consume( ) {
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
