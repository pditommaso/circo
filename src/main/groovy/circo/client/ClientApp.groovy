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
import circo.frontend.FrontEnd
import circo.messages.JobContext
import circo.reply.AbstractReply
import circo.reply.ResultReply
import circo.reply.SubReply
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
import java.util.concurrent.atomic.AtomicInteger
/**
 *  Client application to interact with the cluster
 *
 *  @Author Paolo Di Tommaso
 */
@Slf4j
class ClientApp {

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

            // -- handle a generic command response
            if( message instanceof AbstractReply ) {
                final replyObj = message as AbstractReply
                final sink = responseSinks[ replyObj.ticket ]
                if( !sink ) {
                    log.error "Missing response sink for req: ${replyObj.ticket}"
                    return
                }

                /*
                 * special handling for the ResultReply
                 */
                if( message.class == SubReply ) {
                    def reply = message as SubReply
                    printJobIds(reply)
                    // get the command
                    def cmd = sink.command as CmdSub
                    // re-sync the barrier count based the real number of jobs submitted
                    // note: only when the command have to print out the result - or - is sync
                    if ( cmd.printOutput || cmd.syncOutput ) {
                        sink.delta = reply?.jobIds?.size()
                    }

                    // create the collection to hold all produced context */
                    sink.gatherResults = new ArrayList( reply.jobIds.size() )
                    sink.expectedResults = reply.jobIds.size()
                }
                else if( message.class == ResultReply ) {
                    handleResultReply(message as ResultReply, sink)
                }

                // assign this response to the associated sink obj -- maintains the
                // relationship between the submitted command and the cluster reply
                sink.reply = replyObj
                sink.countDown()
                log.debug "Counting down, remaining: ${sink.getCount()} "

            }

            else {
                log.debug "<!! unhandled message: $message"
            }

        }

        def void printJobIds( SubReply message ) {
            def count = message.jobIds?.size()
            def list = message.jobIds *. toFmtString { "'${it}'" }
            if ( count == 0 ) {
                log.info "Oops. No job submitted"
            }
            else if ( count == 1 ) {
                log.info "Your job ${list[0]} has been submitted"
            }
            else if ( count <= 10 ) {
                log.info "Your jobs ${list.join(',')} have been submitted"
            }
            else {
                log.info "Your jobs ${list[0..9].join(',')}.. and ${count-10} more have been submitted"
            }
        }

        def void handleResultReply( ResultReply reply, ReplySink sink ) {
            def clazz = sink.command?.class

            // -- print out the job result as requested by the user on the cmdline
            if( clazz == CmdSub ) {
                def cmd = sink.command as CmdSub
                if ( cmd.printOutput && reply.result?.output && ReplySink.currentSink) {
                    print reply.result.output
                }

                // re-sync context
                sink.expectedResults -= 1
                sink.gatherResults << reply.result.context

                if ( sink.expectedResults == 0  ) {
                    def newContext = JobContext.copy(cmd.context)
                    sink.gatherResults.each { JobContext delta ->
                        newContext += delta
                    }
                    // apply the new context
                    app.context = newContext
                }

            }
            else if( clazz == CmdGet && reply.result?.output && ReplySink.currentSink) {
                print reply.result?.output
            }
        }

    }

    /**
     * Holds the responses received from the server
     */
    static class ReplySink {

        static ReplySink currentSink

        AbstractCommand command

        AbstractReply reply

        /* gather all the context produced by the job execution */
        List gatherResults

        int expectedResults

        AtomicInteger delta = new AtomicInteger(0)

        def void setDelta( int value ) {
            if ( value < 0 ) {
                Math.abs(value).times { barrier.countDown() }
            }
            else {
                delta.set(value)
            }
        }

        @Delegate
        CountDownLatch barrier

        def void countDown() {
            if ( delta.get()>0 ) {
                delta.decrementAndGet()
            }
            else {
                barrier.countDown()
            }
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

    final Map<UUID, ReplySink> responseSinks = new HashMap<>()

    final ActorSystem system

    final ActorRef frontEnd

    final ActorRef client

    private AppOptions options

    private JCommander cmdParser

    private CommandParser parsedCommand

    private ConsoleReader console

    private ClusterDaemon localDaemon

    ConsoleReader getConsole() { console }

    JobContext context

    Map<String,String> aliases


    // -- common initialization
    {
        console = new ConsoleReader()
        def historyFile = new File(Consts.APP_HOME_DIR, "history")
        console.history = new FileHistory(historyFile)
        console.historyEnabled = true

        context = new JobContext()
        aliases = new LinkedHashMap<String, String>()
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
        if ( options.version ) {
            println CircoHelper.version()
            System.exit(0)
        }
        else if ( options.fullVersion ) {
            println CircoHelper.version(true)
            System.exit(0)
        }

        // print the logo only when it will enter in interactive mode
        if ( !parsedCommand.hasCommand() ) {
            print Consts.LOGO
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
        if( host?.toLowerCase() in Consts.LOCAL_NAMES || !host ) {
            host = InetAddress.getLocalHost().getHostAddress()
        }

        /*
         * if running in local mode, run a local cluster daemon
         */
        if( options.local ) {
            localDaemon = ClusterDaemon.start('--local', '-p', options.cpu.toString())
            host = CircoHelper.fmt(localDaemon.getSelfAddress())
        }


        Address remoteAddress = CircoHelper.parseAddress(host)
        log.info "Connecting to cluster at ${CircoHelper.fmt(remoteAddress)}"

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
    private ReplySink createSinkForCommand( AbstractCommand cmd ) {
        assert cmd
        assert cmd.ticket

        def numOfReplies = cmd.expectedReplies()
        log.debug "Sub expected numOfReplies: $numOfReplies"

        def holder = new ReplySink()
        holder.command = cmd

        holder.barrier = new CountDownLatch(numOfReplies)

        this.responseSinks.put( cmd.ticket, holder )
        return holder
    }

    /**
     * Send a command request and await for the reply
     */
    def <R extends AbstractReply, T extends AbstractCommand> R send( T command ) {

        // -- assign to this request a UUID
        command.ticket = UUID.randomUUID()

        // -- create the request
        def holder = createSinkForCommand(command)

        // -- submit it - and - reply to the client actor
        frontEnd.tell( command, client )
        holder.await()

        return holder.reply
    }



    def void close(int exitCode = 0) {
        // save the console history
        if( console.history instanceof FileHistory ) {
            (console.history as FileHistory).flush()
        }
        // shutdown Akka
        system.shutdown()

        // local daemon
        if ( localDaemon ) { localDaemon.stop() }

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
            println ""

            while( true ) {
                def line = console.readLine("${Consts.APP_NAME}> ")
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

                else if ( line =~~ /!\d+/ ) {
                    line = console.history.get( line.substring(1).toInteger() )
                    executeCmdLine(line?.toString())
                }
                else if ( line == '!!' ) {
                    line = console.history.last()
                    executeCmdLine(line?.toString())
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
                if ( ReplySink.currentSink ) {
                    ReplySink.currentSink.cancel()
                    // TODO +++ propagate cancel to the target job
                }

            } as SignalHandler  )
        }
        catch( Exception e ) {
            log.warn ("Cannot install term signal handler 'INT'", e)
        }
    }



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

