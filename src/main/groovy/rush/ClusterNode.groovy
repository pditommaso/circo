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
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.UntypedActorFactory
import akka.cluster.Cluster
import com.beust.jcommander.JCommander
import com.beust.jcommander.ParameterException
import com.hazelcast.config.ClasspathXmlConfig
import com.hazelcast.config.Config as HazelcastConfig
import com.hazelcast.core.Hazelcast
import groovy.util.logging.Slf4j
import rush.data.DataStore
import rush.data.HazelcastDataStore
import rush.data.LocalDataStore
import rush.data.WorkerRef
import rush.frontend.FrontEnd
import rush.ui.TerminalUI
import rush.utils.CmdLine
import rush.utils.LoggerHelper

/**
 *  Launch a cluster node instance
 *
 *  @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */


@Slf4j
public class ClusterNode {

    def ActorSystem system
    def DataStore dataStore

    protected CmdLine cmdLine

    /**
     * The cluster node constructor
     *
     * @param cmdLine A object containing the parse CLI parsed arguments
     */
    def ClusterNode(CmdLine cmdLine) {
        this.cmdLine = cmdLine

        System.setProperty("akka.remote.netty.port", String.valueOf(cmdLine.port));
        System.setProperty("akka.remote.netty.hostname", cmdLine.server)

        if( cmdLine.local ) {
            log.info "Running in local mode (no distributed data structures)"
            this.dataStore = new LocalDataStore()
        }
        else {
            log.debug "Launching Hazelcast"
            HazelcastConfig cfg = new ClasspathXmlConfig('hazelcast.xml')
            dataStore = new HazelcastDataStore(Hazelcast.newHazelcastInstance(cfg))
        }

    }

    /*
     * The node run method
     */
    def void run () {
        log.debug "++ Entering run method"
        system = ActorSystem.create("ClusterSystem");
        Cluster cluster = Cluster.get(system)
        def address = cluster.selfAddress()
        WorkerRef.init(system, address)

        // -- create the required actors
        createMasterActor()
        createFrontEnd()
        createProcessors()

        if( cmdLine.interactive ) {
            createTerminalUI()
        }

        log.info "Rush started [${address}]"
    }


    protected void createMasterActor() {
        system.actorOf( new Props({ new JobMaster(dataStore) } as UntypedActorFactory) , JobMaster.ACTOR_NAME)
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
            def props = new Props({ new JobProcessor(store: dataStore, slow: cmdLine.slow)} as UntypedActorFactory)
            system.actorOf( props, "processor$it" )
        }

    }

    /*
     * Stop everything
     */
    def stop() {

        if( system ) try {
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

        log.debug ">> Launching RushServer"
        def node = new ClusterNode(cmdLine)
        try {
            node.run()
        }
        catch( Throwable e ) {
            node.stop()
            log.error("Unable to start Rush", e )
            System.exit(1)
        }

    }

}
