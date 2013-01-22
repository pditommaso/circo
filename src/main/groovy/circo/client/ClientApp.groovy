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

package circo.client
import akka.actor.*
import circo.ClusterDaemon
import circo.Consts
import circo.client.cmd.*
import circo.data.WorkerRef
import circo.frontend.AbstractResponse
import circo.frontend.CmdSubResponse
import circo.frontend.FrontEnd
import circo.messages.JobResult
import circo.util.CircoHelper
import circo.util.LoggerHelper
import com.beust.jcommander.JCommander
import com.typesafe.config.ConfigFactory
import groovy.util.logging.Slf4j
import jline.console.ConsoleReader
import jline.console.history.FileHistory
import sun.misc.Signal
import sun.misc.SignalHandler

import java.util.concurrent.CountDownLatch
/**
 *  Client application to interact with the cluster
 *
 *  @Author Paolo Di Tommaso
 *
 */
@Slf4j
class ClientApp {

    static final File MOKE_HOME

    static {
        MOKE_HOME = new File( System.getProperty("user.home"), ".${Consts.APPNAME}" )
        if( !MOKE_HOME.exists() && !MOKE_HOME.mkdir() ) {
            throw new IllegalStateException("Cannot create path '${MOKE_HOME}' -- Check the file sytem access permission")
        }
    }

    /**
     * Actor that receives the reply message from the cluster
     */
    class ClientActor extends UntypedActor {

        ClientApp app

        def ClientActor( def app ) {
            this.app = app
        }

        def void onReceive( def message ) {

            log.debug "<- $message"

            /*
             * print the Submit response to the stdout
             */
            if( message instanceof CmdSubResponse ) {
                log.info "Your job ${message.ticket} has been submitted"
            }

            // -- handle a generic command response
            if( message instanceof AbstractResponse ) {
                final response = message as AbstractResponse
                final sink = responseSinks [ response.ticket  ]
                if( !sink ) {
                    log.error "Missing response sink for req: ${response.ticket}"
                    return
                }

                // assign this response to the associated sink obj -- maintains the
                // relationship between the submitted command and the cluster reply
                sink.response = response
                sink.countDown()
                log.debug "Counting down, remaining: ${sink.getCount()} "
            }

            /**
             * Collect all the job result for a specific request
             */
            else if( message instanceof JobResult ) {
                final jobResult = message as JobResult
                final ticket = jobResult.jobId.ticket
                log.debug "Result ticket: $ticket"
                final sink = responseSinks[ ticket ]
                if( !sink ) {
                    log.error "Missing holder for ticket: ${ticket}"
                    return
                }

                // -- print out the job result as requested by the user on the cmdline
                if( sink.command instanceof CmdSub && (sink.command as CmdSub).printOutput ) {
                   print jobResult.output
                }

                // notify the acquired result
                sink.countDown()
                log.debug "Counting down, remaining: ${sink.getCount()} "
            }


            else {
                log.debug "<!! unhandled message: $message"
            }

        }

    }

    /**
     * Holds the responses received from the server
     */
    static class ResponseSink {

        static ResponseSink currentSink

        AbstractCommand command

        AbstractResponse response

        @Delegate
        CountDownLatch barrier

        def void countDown() {
            barrier.countDown()
        }

        def void await() {
            currentSink = this
            try {
                barrier.await()
            }
            finally {
                currentSink = null
            }
        }

        def void cancel() {
            while( barrier.getCount() ) barrier.countDown()
        }

    }

    final Map<String, ResponseSink> responseSinks = new HashMap<>()

    final ActorSystem system

    final ActorRef frontEnd

    final ActorRef client

    private AppOptions options

    private JCommander cmdParser

    private CommandParser parsedCommand

    private ConsoleReader console

    private ClusterDaemon localDaemon

    ConsoleReader getConsole() { console }


    // -- common initialization
    {
        console = new ConsoleReader()
        def historyFile = new File(MOKE_HOME, "history")
        console.history = new FileHistory(historyFile)
        console.historyEnabled = true
    }


    /**
     * Client constructor
     *
     * @param args
     * @return
     */
    def ClientApp( String[] args ) {

        // define the command parser structure
        options = new AppOptions()
        cmdParser = AppCommandsFactory.create(options)

        // parse the command line -- throw an exception in case of wrong parameter
        parsedCommand = new CommandParser(cmdParser, args)
        if( parsedCommand.hasFailure() ) {
            throw new AppOptionsException(parsedCommand)
        }
        if( parsedCommand.help ) {
            throw new AppHelpException(parsedCommand)
        }

        // NOW -- the real program initialization
        def akkaConf = """
            akka.event-handlers = ["akka.event.slf4j.Slf4jEventHandler"]
            akka.actor.provider: akka.remote.RemoteActorRefProvider
            akka.remote.netty.port: ${options.port}
        """

        system = ActorSystem.create("CircoClient", ConfigFactory.parseString(akkaConf) )
        WorkerRef.init(system)

        def host = options.remoteHost?.toLowerCase() ?: System.getenv('CIRCO_HOST')
        if( host?.toLowerCase() in ['local','localhost']) {
            host = InetAddress.getLocalHost().getHostAddress()
        }

        /*
         * if running in local mode, run a local cluster daemon
         */
        if( options.local ) {
            localDaemon = ClusterDaemon.start('--local', '-p', '2')
            host = CircoHelper.fmt(localDaemon.getSelfAddress())
        }

        if ( !host ) {
            throw new AppOptionsException( parsedCommand, 'Please specify the remote cluster to which connect using the --host cli option' )
        }

        Address remoteAddress = CircoHelper.fromString(host)
        log.debug "Connecting remote cluster at '$remoteAddress'"

        String remoteActor = "${remoteAddress}/user/${FrontEnd.ACTOR_NAME}"
        log.debug "Remote front-end actor path: $remoteActor"

        frontEnd = system.actorFor(remoteActor)
        client = system.actorOf( new Props( { new ClientActor(this) } as UntypedActorFactory ) )


        installSignalHandlerForCtrl_C()
    }

    /**
     * Create a new command request
     *
     * @param command
     * @param expectedReply
     * @return
     */
    private ResponseSink createSink( AbstractCommand command, int expectedReply = 1 ) {
        log.debug "Creating new $command"
        assert command
        assert command.ticket

        def result = new ResponseSink()
        result.command = command
        result.barrier = expectedReply ? new CountDownLatch(expectedReply) : null

        this.responseSinks.put( command.ticket, result )
        return result
    }


    /**
     * Create a new Submit command request
     */
    private ResponseSink createSinkForSubmit( CmdSub submit ) {
        assert submit
        assert submit.ticket

        def numOfJobs = submit.times?.size() ?: 1
        log.debug "Sub expected numOfJobs: $numOfJobs"

        def holder = new ResponseSink()
        holder.command = submit

        // add 1 because for each job request we got at lest the submit ack + the job results
        def count = (submit.sync || submit.printOutput ? numOfJobs+1 : 1)
        log.debug "Barrier count: ${count}"
        holder.barrier = new CountDownLatch(count)

        this.responseSinks.put( submit.ticket, holder )
        return holder
    }

    /**
     * Send a command request and await for the reply
     */
    def <R extends AbstractResponse, T extends AbstractCommand> R send( T command ) {

        // -- assign to this request a UUID
        command.ticket = UUID.randomUUID().toString()

        // -- create the request
        def holder
        if( command instanceof CmdSub ) {
            holder = createSinkForSubmit(command as CmdSub)
        }
        else {
            holder = createSink(command)
        }

        // -- submit it - and - reply to the client actor
        frontEnd.tell( command, client )
        holder.await()

        return holder.response
    }



//
//    /**
//     * Send a message to the server and wait for a synchronous reply
//     *
//     * @param message
//     * @return
//     */
//    def <T> T askAndWait( def AbstractCommand message ) {
//
//        // create a unique ID for this request
//        message.reqId = UUID.randomUUID()
//
//        // send the message and wait for the reply
//        def duration = Duration.create('2 second')
//        def future = Patterns.ask(frontEnd, message, duration.toMillis())
//        T result = Await.result(future, duration) as T
//
//        return result
//    }
//
//    @Deprecated
//    def void askAndWait( def message, Closure complete ) {
//        def duration = Duration.create('1 second')
//        def result = AkkaHelper.ask(frontEnd, message, duration.toMillis())
//        complete.call(result)
//    }
//
//
//    @Deprecated
//    def void submit( def message ) {
//        frontEnd.tell( message, client )
//    }
//
//    @Deprecated
//    def void submit( CmdSub sub ) {
//
//        def count = sub.sync ? 2 : 1
//        receivedSignal = new CountDownLatch(count)
//        frontEnd.tell( sub, client )
//        receivedSignal.await()
//    }
//
//    @Deprecated
//    def ask( def message ) {
//        AkkaHelper.ask(frontEnd, message, 1000)
//    }


    def void close(int exitCode = 0) {
        // save the console history
        if( console.history instanceof FileHistory ) {
            (console.history as FileHistory).flush()
        }
        // shutdown Akka
        system.shutdown()

        // local daemon
        if ( localDaemon ) localDaemon.stop()

        // .. bye
        System.exit(exitCode)
    }


    def void run() {

        // -- run the command and exit
        if( parsedCommand.hasCommand() ) {
            log.debug "Executing command: $parsedCommand"
            executeCommand(parsedCommand)
        }

        // entering in interactive mode
        // that means wait the user to enter a command and execute it
        else {
            log.debug "Entering interactive mode"

            print LOGO

            while( true ) {
                def line = console.readLine("${Consts.APPNAME}> ")
                log.trace "Loop: $line"

                if( !line ) {
                    continue
                }
                else if ( line.toLowerCase() == 'exit' ) {
                    break
                }

                else if ( line.toLowerCase() in ['?','help'] ) {
                    printAvailableCommands()
                }

                else if ( line ){
                    executeCmdLine(line)
                }

                console.history.add(line)

            }
        }
    }

    /**
     * Prints out the available commands
     */
    private void printAvailableCommands() {

        def commands = cmdParser.getCommands().keySet()
        log.debug "Commands: ${commands}"

        def max = 0; commands.each { max = Math.max(max, it.length()) }
        log.debug "maxLength: $max "

        println "Commands:"

        commands.each { String cmd ->
            println " ${cmd.padRight(max)}  ${cmdParser.getCommandDescription(cmd)}"
        }
    }

    /**
     * Parse the entered text line and execute the resulting command
     *
     * @param line The string command including any option/argument
     */
    private void executeCmdLine( String line ) {
        try {
            assert line
            def parser = CommandParser.parse(line)
            executeCommand( parser )
        }
        catch( Throwable err ) {
            log.error("Unexpected error executing : '$line'", err)
        }

    }

    private void executeCommand( CommandParser parser ) {

        parser.with {

            if( isHelp() ) {
                printHelp()
            }

            else if ( hasFailure() ) {
                printFailureMessage()
            }

            // try to execute the command BUT the failure
            else if( hasCommand() ) {
                try {
                    getCommand().execute(this)
                }
                catch( IllegalArgumentException err ) {
                    // show the exception message
                    println err.getMessage() ?: err?.toString() ?: "Unknown in command: $parser"

                    printHelp()
                }

            }

        }
    }


    private void installSignalHandlerForCtrl_C(def shell) {

        try {
            Signal.handle(new Signal("INT"), {
                log.debug("Interrupting current thread .. ")
                if ( ResponseSink.currentSink ) {
                    ResponseSink.currentSink.cancel()
                    // TODO +++ propagate cancel to the target job
                }

            } as SignalHandler  )
        }
        catch( Exception e ) {
            log.warn ("Cannot install term signal handler 'INT'", e)
        }
    }

    /**
     * The ascii art logo, displayed on start
     */
    def static LOGO = """\
       ___ _
      / __(_)_ _ __ ___
     | (__| | '_/ _/ _ \\
      \\___|_|_| \\__\\___/

    """
    .stripIndent()


    /**
     * The client main method
     *
     * @param args The CLI arguments
     */
    static void main( String[] args ) {
        // configure the logging system
        def cli = LoggerHelper.configureClientLogger(args)

        // launch the application
        def exitCode = 0
        def app = null
        try {
            app = new ClientApp(cli as String[])
            app.run()
        }
        catch( AppHelpException failure ) {
            // show the program usage text description
            println failure.getMessage()
        }

        catch( AppOptionsException failure ) {
            // report an error code
            exitCode = 1
            // print the error message
            log.error(failure.getMessage())
        }

        catch( Exception failure ) {
            log.error( "Unhandled error", failure)
            exitCode = 2
        }

        finally {
            if(app) app.close()
        }

        log.debug ">> exitcode: $exitCode <<"
        System.exit(exitCode)

    }
}

