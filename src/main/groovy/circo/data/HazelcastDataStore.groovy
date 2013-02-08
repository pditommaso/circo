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
import java.util.concurrent.locks.Lock

import circo.model.TaskEntry
import circo.model.TaskId
import circo.model.TaskStatus
import com.hazelcast.config.ClasspathXmlConfig
import com.hazelcast.config.Config
import com.hazelcast.config.Join
import com.hazelcast.config.MapConfig
import com.hazelcast.config.MapIndexConfig
import com.hazelcast.config.MapStoreConfig
import com.hazelcast.core.AtomicNumber
import com.hazelcast.core.EntryEvent
import com.hazelcast.core.EntryListener
import com.hazelcast.core.Hazelcast
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.hazelcast.query.SqlPredicate
import com.typesafe.config.Config as TypesafeConfig
import com.typesafe.config.ConfigException
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */

@Slf4j
@CompileStatic
class HazelcastDataStore extends AbstractDataStore {

    private HazelcastInstance hazelcast

    private Map<Closure,EntryListener> listenersMap = [:]

    private AtomicNumber idGen

    private AtomicNumber nodeIdGen

    /**
     * Create an {@code com.hazelcast.core.HazelcastInstance} with configuration
     * properties provided by the 'application.conf' settings
     *
     */
    def HazelcastDataStore( TypesafeConfig appConfig, List<String> clusterMembers = null, boolean multiCast = false ) {

        init(createInstance(appConfig, clusterMembers, multiCast))

    }


    @Override
    void shutdown() {
        hazelcast.lifecycleService.shutdown()
    }

    /*
     * Parse the configuration settings and create the
     * {@code com.hazelcast.core.HazelcastInstance}  accordingly
     */
    private HazelcastInstance createInstance(TypesafeConfig appConfig, List<String> clusterMembers, boolean multiCast ) {

        /*
         * general hazelcast configuration
         */
        def cfg
        try {
            cfg = new ClasspathXmlConfig("hazelcast.xml")
            log.debug "Using Hazelcast configuration found on classpath"
        }
        catch( Exception e ) {
            cfg = new Config()
        }
        cfg.setProperty("hazelcast.logging.type", "slf4j")

        /*
         * network conf
         */
        def Join join = cfg.getNetworkConfig().getJoin()
        if( multiCast ) {
            log.debug "Hazelcast -- enabling multicast"
            join.getTcpIpConfig().setEnabled(false)
            join.getMulticastConfig().setEnabled(true)
        }

        if ( clusterMembers ) {
            log.debug "Hazelcast -- adding TCP members: $clusterMembers"
            clusterMembers.each { String it ->
                join.getTcpIpConfig().addMember( it )
            }
        }

        /*
         * configure the JDBC persistence if provided in the configuration file
         */
        def mapStoreConfig = null
        try {
            def storeConfig = appConfig.getConfig('store.jdbc')
            log.info "Setting up JDBC store persistence"
            JdbcJobsMapStore.dataSource = JdbcDataSourceFactory.create(storeConfig)

            mapStoreConfig = new MapStoreConfig()
                    .setClassName( JdbcJobsMapStore.getName() )
                    .setEnabled(true)

        }
        catch( ConfigException.Missing e ) {
            log.debug "No store persistence provided"
        }

        /*
         * TASKS map configuration
         */

        def jobsConfig = new MapConfig('tasks')
                .addMapIndexConfig( new MapIndexConfig('id',false) )
                .addMapIndexConfig( new MapIndexConfig('status',false) )
                .addMapIndexConfig( new MapIndexConfig('ownerId', false) )

        if ( mapStoreConfig ) {
            jobsConfig.setMapStoreConfig(mapStoreConfig)
        }

        cfg.addMapConfig(jobsConfig)


        /*
         * let's create the Hazelcast instance obj
         */
        Hazelcast.newHazelcastInstance(cfg)
    }

    /**
     * Create a datastore with the provided {@code HazelcastInstance}
     * -- specify {@code null} for a local instance, useful for testing purpose
     */
    def HazelcastDataStore( HazelcastInstance instance = null ) {

        if( !instance ) {
            log.warn "Using TEST Hazelcast instance"
            instance = Hazelcast.newHazelcastInstance(null)
        }

        // initialize the data structure
        init(instance)
    }

    protected void init( HazelcastInstance instance ) {

        this.hazelcast = instance
        this.jobsMap = hazelcast.getMap('tasks')
        this.nodeDataMap = hazelcast.getMap('nodeInfo')
        this.idGen = hazelcast.getAtomicNumber('idGenerator')
        this.nodeIdGen = hazelcast.getAtomicNumber('nodeIdGen')
    }

    TaskId nextTaskId() { new TaskId( idGen.addAndGet(1) ) }

    int nextNodeId() { nodeIdGen.addAndGet(1) }


    @Override
    protected Lock getLock(def key) {
        hazelcast.getLock(key)
    }

    List<TaskEntry> findTasksById( final String taskId) {
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

        def result = (jobsMap as IMap) .values(new SqlPredicate(criteria))
        new ArrayList<TaskEntry>(result as Collection<TaskEntry>)
    }

    @Override
    List<TaskEntry> findAllTasksOwnerBy(Integer nodeId) {
        assert nodeId

        def criteria = "ownerId = $nodeId"
        def result = (jobsMap as IMap) .values(new SqlPredicate(criteria))
        new ArrayList<TaskEntry>(result as Collection<TaskEntry>)

    }

    List<TaskEntry> findTasksByStatus( TaskStatus[] status ) {
        assert status

        def criteria = new SqlPredicate("status IN (${status.join(',')})  ")
        def result = (jobsMap as IMap) .values(criteria)
        new ArrayList<TaskEntry>(result as Collection<TaskEntry>)

    }


    void addNewTaskListener(Closure callback) {
        assert callback

        def entry = new EntryListener() {
            @Override
            void entryAdded(EntryEvent event) { callback.call(event.getValue()) }

            @Override
            void entryRemoved(EntryEvent event) { }

            @Override
            void entryUpdated(EntryEvent event) { }

            @Override
            void entryEvicted(EntryEvent event) { }
        }

        listenersMap.put(callback, entry)
        (jobsMap as IMap) .addLocalEntryListener(entry)

    }


    void removeNewTaskListener( Closure listener ) {
        def entry = listenersMap.get(listener)
        if ( !entry ) { log.warn "No listener registered for: $listener"; return }
        (jobsMap as IMap).removeEntryListener(entry)
    }


}
