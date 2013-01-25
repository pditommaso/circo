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

import circo.messages.JobEntry
import circo.messages.JobId
import circo.messages.JobStatus
import com.hazelcast.config.*
import com.hazelcast.core.*
import com.hazelcast.query.SqlPredicate
import com.typesafe.config.Config as TypesafeConfig
import com.typesafe.config.ConfigException
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import java.util.concurrent.locks.Lock

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

    /**
     * Create an {@code com.hazelcast.core.HazelcastInstance} with configuration
     * properties provided by the 'application.conf' settings
     *
     */
    def HazelcastDataStore( TypesafeConfig appConfig, List<String> clusterMembers = null, boolean multiCast = false ) {

        init(createInstance(appConfig, clusterMembers, multiCast))

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
         * JOBS map configuration
         */


        def jobsConfig = new MapConfig('jobs')
                .addMapIndexConfig( new MapIndexConfig('id',false) )
                .addMapIndexConfig( new MapIndexConfig('status',false) )

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
        this.jobsMap = hazelcast.getMap('jobs')
        this.nodeDataMap = hazelcast.getMap('nodeInfo')
        this.idGen = hazelcast.getAtomicNumber('idGenerator')
    }

    JobId nextJobId() { new JobId( idGen.addAndGet(1) ) }


    @Override
    protected Lock getLock(def key) {
        hazelcast.getLock(key)
    }

    List<JobEntry> findJobsById( final String jobId ) {
        assert jobId

        boolean likeOp = false
        def value

        if ( jobId.contains('*') ) {
            value = jobId.replace('*','%')
            likeOp = true
        }
        else {
            value = jobId
        }

        def criteria = likeOp ? "id.toHexString() LIKE '$value'" : "id.toHexString() = '$value'"

        def result = (jobsMap as IMap) .values(new SqlPredicate(criteria))
        new ArrayList<JobEntry>(result as Collection<JobEntry>)
    }


    List<JobEntry> findJobsByStatus( JobStatus[] status ) {
        assert status

        def criteria = new SqlPredicate("status IN (${status.join(',')})  ")
        def result = (jobsMap as IMap) .values(criteria)
        new ArrayList<JobEntry>(result as Collection<JobEntry>)

    }

    void addNewJobListener(Closure callback) {
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

        (jobsMap as IMap) .addLocalEntryListener(entry)

        listenersMap.put(callback, entry)
    }


    void removeNewJobListener( Closure listener ) {
        def entry = listenersMap.get(listener)
        if ( !entry ) { log.warn "No listener registered for: $listener"; return }
        (jobsMap as IMap).removeEntryListener(entry)
    }



}
