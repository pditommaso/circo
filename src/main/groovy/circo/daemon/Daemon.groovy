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
import akka.actor.ActorSystem
import akka.actor.Address
import akka.actor.Props
import akka.actor.UntypedActorFactory
import akka.cluster.Cluster
import circo.Const
import circo.data.DataStore
import circo.data.HazelcastDataStore
import circo.data.LocalDataStore
import circo.model.WorkerRef
import circo.frontend.FrontEnd
import circo.ui.TerminalUI
import circo.util.CircoHelper
import circo.util.CmdLine
import circo.util.LoggerHelper
import com.beust.jcommander.JCommander
import com.beust.jcommander.ParameterException
import com.typesafe.config.ConfigFactory
import groovy.util.logging.Slf4j
/**
 *  Launch a cluster node instance
 *
 *  @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */


@Slf4j
public class Daemon {

    private ActorSystem system

    private Cluster cluster

    private DataStore dataStore

    protected CmdLine cmdLine

    List<Address> nodes

    Address selfAddress

    boolean stopped

    def boolean isStopped() { stopped }

    private multiCast = false



    /**
     * The cluster node constructor
     *
     * @param cmdLine A object containing the parse CLI parsed arguments
     */
    def Daemon(CmdLine cmdLine) {
        this.cmdLine = cmdLine

        if( cmdLine.local ) {
            log.debug "Using local mode"
            multiCast = true
            nodes = parseAddresses(Const.LOCAL_ADDRESS)
        }

        else if ( cmdLine.join ) {
            log.debug "Using join mode: ${cmdLine?.join}"

            if ( cmdLine?.join == 'auto') {
                nodes = parseAddresses( InetAddress.getLocalHost().getHostAddress() )
                multiCast = true
            }
            else if( cmdLine?.join?.startsWith('auto:') ) {
                multiCast = true
                nodes = parseAddresses(cmdLine.join.substring('auto:'.size()))
            }
            else {
                nodes = parseAddresses(cmdLine.join)
            }
        }

    }

    static private List<Address> parseAddresses( String addresses ) {
        def result  = []

        if( !addresses ) return result

        addresses.eachLine { String line ->
            line.split('[,\b]').each { String it -> result << (Address)CircoHelper.parseAddress(it) }
        }

        return result
    }

    /*
     * Initialize the Akka system and the Hazelcast data container
     *
     */
    def void init() {

        def daemonAddress = cmdLine.local ? Const.LOCAL_ADDRESS : cmdLine.host
        log.debug "Configuring host: ${daemonAddress}:${cmdLine.port}"

        /*
         * sets some Akka properties
         */
        System.setProperty("akka.remote.netty.port", String.valueOf(cmdLine.port));
        System.setProperty("akka.remote.netty.hostname", daemonAddress )
        //System.setProperty('akka.cluster.auto-join','off')

        /*
         * set the Sigar library path
         */
        String sigarPath = System.properties['sigar.lib.path']
        if ( sigarPath ) {
            log.debug "Setting Sigar libs path to: '$sigarPath'"
            def libsPath = System.properties['java.library.path']
            libsPath = libsPath ? "$libsPath:${new File(sigarPath).absolutePath}" : new File(sigarPath).absolutePath
            System.setProperty('java.library.path', libsPath)
        }
        else {
            log.warn( "Missing Sigar libraries path -- Make sure run script define the Java property 'sigar.lib.path'" )
        }

        /*
         * Create the Akka system
         */
        system = ActorSystem.create(Const.DEFAULT_AKKA_SYSTEM);
        cluster = Cluster.get(system)
        selfAddress = cluster.selfAddress()
        joinNodes(cluster)
        WorkerRef.init(system, selfAddress)


        /*
         * Create the in-mem data store
         */
        if( cmdLine.local ) {
            log.info "Running in local mode (no distributed data structures)"
            this.dataStore = new LocalDataStore()
        }
        else {
            log.debug "Launching Hazelcast (1) -- multicast: ${multiCast} - nodes: $nodes"
            List<String> members = nodes.collect { Address it -> it.host().get() }
            def itself = cluster.selfAddress().host().get()
            if ( !members.contains(itself)) { members.add(itself) }
            log.debug "Launching Hazelcast (2) -- members: ${members}"
            dataStore = new HazelcastDataStore( ConfigFactory.load(), members, multiCast )
        }

    }

    /*
     * try to join a random node in the cluster
     */
    def void joinNodes( Cluster cluster ) {
        if ( !nodes ) { return }

        def list = new ArrayList<Address>(nodes)
        while( list ) {
            def addr = list.remove( new Random().nextInt( list.size() )  )
            if( addr == selfAddress) {
                log.debug "Skipping joining to itself"
                continue
            }

            try {
                log.info "Joining cluster node: $addr"
                cluster.join(addr)
                break
            }
            catch( Exception e ) {
                log.error "Failed to join cluster at node: $addr", e
                sleep(1000)
            }
        }

    }


    /*
     * The node run method
     */
    def void run () {
        log.debug "++ Entering run method"

        // -- create the required actors
        createMasterActor()
        createFrontEnd()
        createProcessors()

        if( cmdLine.interactive ) {
            createTerminalUI()
        }

        log.info "Circo node started [${cluster.selfAddress()}]"
    }


    protected void createMasterActor() {
        system.actorOf( new Props({ new NodeMaster(dataStore) } as UntypedActorFactory) , NodeMaster.ACTOR_NAME)
    }

    protected void createFrontEnd() {
        system.actorOf( new Props({ new FrontEnd(dataStore) } as UntypedActorFactory), FrontEnd.ACTOR_NAME )
    }

    protected void createTerminalUI() {
        system.actorOf( new Props({ new TerminalUI(dataStore)} as UntypedActorFactory), TerminalUI.ACTOR_NAME )
    }

    // -- finally create the workers
    protected void createProcessors() {

        def nCores = cmdLine.processors
        nCores.times {
            def props = new Props({ new TaskProcessor(store: dataStore, slow: cmdLine.slow)} as UntypedActorFactory)
            system.actorOf( props, "processor$it" )
        }

    }

    /*
     * Stop everything
     */
    def stop() {
        if( stopped ) return
        stopped = true

        if( !system ) return

        try {
            system.shutdown()
        }
        catch( Throwable e ) {
            log.warn("Unable to stop Akka System", e)
        }
    }



    /**
     * Parse the command line argument and return an instance of {@code CmdLine}
     *
     * @param args The array or cli arguments as specified by the suer
     * @return An instance of
     */
    static CmdLine parseCmdLine(String[] args) {

        /*
         * Parse the command line args
         */
        def cmdLine = new CmdLine()
        def commander = new JCommander(cmdLine)
        try {
            commander.parse(args)
        }
        catch( ParameterException e ) {
            println e.getMessage()
            println ""
            commander.usage()
            System.exit(1)
        }

        if ( cmdLine.help ) {
            commander.usage()
            System.exit(0)
        }

        return cmdLine
    }

    /**
     * The server node entry point
     *
     * @param args The cli arguments are specified by the user
     * @see CmdLine
     */
    static void main (String[] args) {

        // parse the command line
        def cmdLine = parseCmdLine(args)
        // configure the loggers based on the cli arguments
        LoggerHelper.configureDaemonLogger(cmdLine)

        def node = new Daemon(cmdLine)
        try {
            node.init()
            node.run()
        }
        catch( Throwable e ) {
            node.stop()
            log.error("Unable to start Circo", e )
            System.exit(1)
        }

    }

    static Daemon start( String[] args ) {


        // parse the command line
        def cmdLine = parseCmdLine(args)
        def daemon = new Daemon(cmdLine)
        try {
            println "Starting daemon"
            daemon.init()
            Thread thread = new Thread( {
                try { daemon.run() }
                catch ( Exception e ) { log.error("Unable to run daemon", e); daemon.stop() }
            } as Runnable )
            thread.setName('local-daemon')
            thread.setDaemon(false)
            thread.run()

            sleep(1000)
        }

        catch( Throwable e ) {
            daemon.stop()
            throw new RuntimeException("Unable to start local daemon", e)
        }

        return daemon
    }

}
