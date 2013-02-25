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

package circo.data
import javax.sql.DataSource

import circo.data.sql.JdbcDataSourceFactory
import circo.model.AddressRef
import circo.model.Job
import circo.model.NodeData
import circo.model.TaskEntry
import circo.model.TaskId
import circo.model.TaskStatus
import com.hazelcast.config.ClasspathXmlConfig
import com.hazelcast.config.Config as HzConfig
import com.hazelcast.config.Join
import com.hazelcast.config.MapConfig
import com.hazelcast.config.MapIndexConfig
import com.hazelcast.config.MapStoreConfig
import com.hazelcast.core.AtomicNumber
import com.hazelcast.core.EntryListener
import com.hazelcast.core.Hazelcast
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.hazelcast.query.SqlPredicate
import com.jolbox.bonecp.BoneCPDataSource
import com.typesafe.config.Config as TypesafeConfig
import com.typesafe.config.ConfigException
import groovy.util.logging.Slf4j
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */

@Slf4j
class HazelcastDataStore extends AbstractDataStore {

    private HazelcastInstance hazelcast

    private Map<Closure,EntryListener> listenersMap = [:]

    private AtomicNumber taskIdGen

    private AtomicNumber nodeIdGen

    private HzJdbcJobsMapStore jobsMapStore

    private HzJdbcTasksMapStore tasksMapStore

    private HzJdbcNodesMapStore nodesMapStore

    private DataSource dataSource

    /**
     * Create an {@code com.hazelcast.core.HazelcastInstance} with configuration
     * properties provided by the 'application.conf' settings
     *
     */
    def HazelcastDataStore( TypesafeConfig appConfig, List<AddressRef> clusterMembers = null, boolean multiCast = false ) {

        // Configure the JDBC persistence if provided in the configuration file
        dataSource = configureDataSource(appConfig)

        // Configure and create a Hazelcast instance
        hazelcast = createInstance(appConfig, clusterMembers, multiCast, dataSource != null)

        // initialize the data structures
        init()

    }


    /**
     * Create a data-store with the provided {@code HazelcastInstance}
     * -- specify {@code null} for a local instance, useful for testing purpose
     */
    def HazelcastDataStore( HazelcastInstance instance = null, DataSource dataSource = null ) {

        // Define the Hazelcast to use
        if( instance ) {
            hazelcast = instance
        }
        else {
            log.warn "Using TEST Hazelcast instance"
            hazelcast = Hazelcast.newHazelcastInstance(null)
        }

        // Set the data-source
        this.dataSource = dataSource


        // Initialize the data structure
        init()
    }



    @Override
    void shutdown() {
        /*
         * shutdown Hazelcast
         */
        try {
            hazelcast.lifecycleService.shutdown()
        }
        catch( Exception e ) {
            log.warn e.getMessage()
        }

        /*
         * Shutdown the connection pool
         */
        try {
            if ( dataSource instanceof BoneCPDataSource) {
                (dataSource as BoneCPDataSource).close()
            }
        }
        catch( Exception e ) {
            log.warn e.getMessage()
        }
    }

    /*
     * Parse the configuration settings and create the
     * {@code com.hazelcast.core.HazelcastInstance}  accordingly
     */
    private HazelcastInstance createInstance(TypesafeConfig appConfig, List<AddressRef> clusterMembers, boolean multiCast, boolean hasDataSource ) {

        // Create the main hazelcast configuration object
        HzConfig cfg = createConfig()

        //  Configure network
        configureNetwork(cfg, clusterMembers, multiCast)

        // Configure all map stores
        configureAllMapStores(cfg,hasDataSource)

        // finally create the Hazelcast instance obj
        Hazelcast.newHazelcastInstance(cfg)
    }

    /**
     * Try to load the 'hazelcast.xml' configuration file on the classpath.
     * If it can be loaded create an empty configuration object
     *
     * @return The Hazelcast {@code Config} instance
     */
    protected HzConfig createConfig() {
        HzConfig result
        try {
            result = new ClasspathXmlConfig("hazelcast.xml")
            log.debug "Using Hazelcast configuration found on classpath"
        }
        catch( Exception e ) {
            result = new HzConfig()
        }

        result.setProperty("hazelcast.logging.type", "slf4j")
    }

    /**
     * Try to create a JDBC data-source given the application configuration object
     *
     * @param appConfig
     * @return A {@code DataSource} instance or {@code null} if the JDBC connection has been provided in the {@code application.conf } file
     */
    static protected DataSource configureDataSource( TypesafeConfig appConfig ) {
        def dataSource = null
        try {
            def storeConfig = appConfig.getConfig('store.hazelcast.jdbc')
            log.info "Setting up JDBC store persistence"
            dataSource = JdbcDataSourceFactory.create(storeConfig)
        }
        catch( ConfigException.Missing e ) {
            log.debug "No store persistence provided"
        }

        return dataSource
    }

    /**
     * Configure the Hazelcast network
     *
     * @param cfg
     * @param clusterMembers
     * @param multiCast
     */
    static protected void configureNetwork( HzConfig cfg, List<AddressRef> clusterMembers, boolean multiCast ) {
        /*
         * Network configuration
         */
        def Join join = cfg.getNetworkConfig().getJoin()
        if( multiCast ) {
            log.debug "Hazelcast -- enabling multicast"
            join.getTcpIpConfig().setEnabled(false)
            join.getMulticastConfig().setEnabled(true)
        }

        if ( clusterMembers ) {
            log.debug "Hazelcast -- adding TCP members: $clusterMembers"
            clusterMembers.each { AddressRef it ->
                join.getTcpIpConfig().addMember( it.toString() )
            }
        }
    }

    /**
     * Configure the 'tasks' map
     *
     * @param cfg
     * @param hasMapStore
     */
    static protected void configureTasks( HzConfig cfg, boolean hasMapStore ) {

        // get - or create when missing - the TASKS map configuration
        def mapConfig = cfg.getMapConfig('tasks')
        if( !mapConfig ) {
            log.warn "Missing 'tasks' definition in the 'hazelcast.xml' configuration file -- creating a new map configuration object"
            mapConfig = new MapConfig('tasks')
            cfg.addMapConfig(mapConfig)
        }


        if( !hasMapStore )  {
            mapConfig.addMapIndexConfig( new MapIndexConfig('id',false) )
                    .addMapIndexConfig( new MapIndexConfig('status',false) )
                    .addMapIndexConfig( new MapIndexConfig('ownerId', false) )
        }
        else {

            // set it in the Hazelcast configuration
            mapConfig.setMapStoreConfig( new MapStoreConfig()
                    .setClassName( HzJdbcTasksMapStore.getName() )
                    .setEnabled(true) )
        }

    }

    /**
     * Configure the 'jobs' map
     */
    static protected void configureJobs( HzConfig cfg, boolean hasDataSource ) {

        if( !hasDataSource ) return

        // get - or create when missing - the JOBS map configuration
        def mapConfig = cfg.getMapConfig('jobs')
        if( !mapConfig ) {
            log.warn "Missing 'jobs' definition in the 'hazelcast.xml' configuration file -- creating a new map configuration object"
            mapConfig = new MapConfig('jobs')
            cfg.addMapConfig(mapConfig)
        }

        // set the jdbc data-store
        mapConfig.setMapStoreConfig( new MapStoreConfig()
                .setClassName(HzJdbcJobsMapStore.getName())
                .setEnabled(true) )
    }

    /**
     * Configure the 'jobs' map
     */
    static protected void configureNodesMap( HzConfig cfg, boolean hasDataSource ) {

        if( !hasDataSource ) return

        // get - or create when missing - the NODES map configuration
        def mapConfig = cfg.getMapConfig('nodes')
        if( !mapConfig ) {
            log.warn "Missing 'nodes' definition in the 'hazelcast.xml' configuration file -- creating a new map configuration object"
            mapConfig = new MapConfig('nodes')
            cfg.addMapConfig(mapConfig)
        }

        // set the jdbc data-store
        mapConfig.setMapStoreConfig( new MapStoreConfig()
                .setClassName(HzJdbcNodesMapStore.getName())
                .setEnabled(true) )
    }


    static protected void configureAllMapStores( HzConfig cfg, boolean hasDataSource )  {

        configureJobs(cfg, hasDataSource)
        configureTasks(cfg, hasDataSource)
        configureNodesMap(cfg, hasDataSource)

    }


    protected void init() {

        if ( dataSource ) {
            // set the jdbc data-source
            HzJdbcTasksMapStore.dataSource = dataSource
            HzJdbcNodesMapStore.dataSource = dataSource
            HzJdbcJobsMapStore.dataSource = dataSource

            // create a map store instance
            jobsMapStore = new HzJdbcJobsMapStore()
            tasksMapStore = new HzJdbcTasksMapStore()
            nodesMapStore = new HzJdbcNodesMapStore()
        }


        taskIdGen = hazelcast.getAtomicNumber('taskIdGen')
        nodeIdGen = hazelcast.getAtomicNumber('nodeIdGen')

        jobs = hazelcast.getMap('jobs')
        tasks = hazelcast.getMap('tasks')
        nodes = hazelcast.getMap('nodes')
        queue = hazelcast.getMap('queue')
        files = hazelcast.getMap('files')


    }

    // -------------------------------- JOBS operation -------------------------------------

    @Override
    List<Job> listJobs() {
        if( jobsMapStore ) {
            jobsMapStore.loadAll()
        }
        else {
            super.listJobs()
        }
    }


    // -------------------------------- TASKS operation ------------------------------------

    TaskId nextTaskId() { new TaskId( taskIdGen.addAndGet(1) ) }


    @Override
    List<TaskEntry> findTasksByOwnerId(Integer nodeId) {
        assert nodeId

        if( tasksMapStore ) {
            // use the JDBC finder
            return tasksMapStore.findByOwnerId(nodeId)
        }
        else {
            // fall back on Hazelcast query
            def result = (tasks as IMap) .values(new SqlPredicate("ownerId = $nodeId"))
            return new ArrayList<TaskEntry>(result as Collection<TaskEntry>)
        }

    }

    @Override
    List<TaskEntry> findTasksByStatus( TaskStatus[] status ) {
        assert status

        if( tasksMapStore ) {
            tasksMapStore.findByStatus(status)
        }
        else {
            // fall back on Hazelcast query
            def result = (tasks as IMap) .values(new SqlPredicate("status in ( ${status.join(',')} )"))
            return new ArrayList<TaskEntry>(result as Collection<TaskEntry>)
        }


    }

    @Override
    List<TaskEntry> findTasksByRequestId( UUID requestId ) {
        assert requestId

        if ( tasksMapStore ) {
            tasksMapStore.findByRequestId(requestId)
        }
        else {
            // TODO ++ implements using criteria API and using an Hazelcast index
            super.findTasksByRequestId(requestId)
        }

    }

    @Override
    List<TaskEntry> listTasks() {

        if( tasksMapStore ) {
            tasksMapStore.loadAll()
        }
        else {
            new ArrayList(tasks.values())
        }

    }


    @Deprecated
    List<TaskEntry> findTasksById( final String taskId ) {
        assert taskId

        boolean likeOp = false
        def value

        if ( taskId.contains('*') ) {
            value = taskId.replace('*','%')
            likeOp = true
        }
        else {
            value = taskId
        }

        // remove '0' prefix
        while( value.size()>1 && value.startsWith('0') ) { value = value.substring(1) }

        // the query criteria
        def criteria = likeOp ? "id.toString() LIKE '$value'" : "id.toString() = '$value'"

        def result = (tasks as IMap) .values(new SqlPredicate(criteria))
        new ArrayList<TaskEntry>(result as Collection<TaskEntry>)
    }

    // ----------------------- NODE DATA operations ------------------------------------------


    int nextNodeId() { nodeIdGen.addAndGet(1) }

    @Override
    List<NodeData> listNodes() {

        if( nodesMapStore ) {
            nodesMapStore.loadAll()
        }
        else {
            super.listNodes()
        }
    }



}
