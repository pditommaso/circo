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

package rush.data
import com.hazelcast.config.ClasspathXmlConfig
import com.hazelcast.config.MapConfig
import com.hazelcast.config.MapStoreConfig
import com.hazelcast.core.*
import com.hazelcast.query.SqlPredicate
import com.typesafe.config.Config as TypesafeConfig
import com.typesafe.config.ConfigException
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import rush.messages.JobEntry
import rush.messages.JobStatus

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

    def HazelcastDataStore( TypesafeConfig config ) {

        // -- hazelcast configuration
        def cfg = new ClasspathXmlConfig('hazelcast.xml')

        // -- look for optional JDBC persistence configuration
        try {
            def storeConfig = config.getConfig('store.jdbc')
            log.info "Setting up JDBC store persistence"
            JdbcJobsMapStore.dataSource = JdbcDataSourceFactory.create(storeConfig)

            def mapStoreConfig =
                new MapStoreConfig()
                        .setClassName( JdbcJobsMapStore.getName() )
                        .setEnabled(true)

            cfg.addMapConfig(new MapConfig('jobs').setMapStoreConfig(mapStoreConfig))

        }
        catch( ConfigException.Missing e ) {
            log.debug "No store persistence provided"
        }

        // create the instance - and - initialize it
        init(Hazelcast.newHazelcastInstance(cfg))
    }


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
//TODO ++ index can be added only at the very first instance
//        // add indexes
//        (jobsMap as IMap).addIndex("id", false)
//        (jobsMap as IMap).addIndex("status", false)
    }


    @Override
    protected Lock getLock(def key) {
        hazelcast.getLock(key)
    }

    List<JobEntry> findJobsById( final String jobId) {
        assert jobId

        String ticket
        String index
        int pos = jobId.indexOf(':')
        if( pos == -1 ) {
            ticket = jobId
            index = null
        }
        else {
            String[] slice = jobId.split('\\:')
            ticket = slice[0]
            index = slice.size()>1 ? slice[1] : null
        }

        // replace the '*' with sql '%'
        ticket = ticket.replaceAll('\\*','%')
        if ( index ) {
            index = index.replaceAll('\\*','%')
        }

        if ( !ticket.contains('%') ) {
            ticket += '%'
        }


        def match = index ? "$ticket:$index" : ticket
        def criteria = "id.toString() LIKE '$match'"

        def result = (jobsMap as IMap) .values(new SqlPredicate(criteria))
        new ArrayList<JobEntry>(result as Collection<JobEntry>)
    }


    List<JobEntry> findJobsByStatus( JobStatus[] status ) {
        assert status

        def criteria = new SqlPredicate("status IN (${status.join(',')})  ")
        def result = (jobsMap as IMap) .values(criteria)
        new ArrayList<JobEntry>(result as Collection<JobEntry>)

    }

    void addNewJobListener(Closure listener) {
        assert listener

        def entry = new EntryListener() {
            @Override
            void entryAdded(EntryEvent event) { listener.call(event.getValue()) }

            @Override
            void entryRemoved(EntryEvent event) { }

            @Override
            void entryUpdated(EntryEvent event) { }

            @Override
            void entryEvicted(EntryEvent event) { }
        }

        (jobsMap as IMap) .addLocalEntryListener(entry)
        listenersMap.put(listener, entry)
    }


    void removeNewJobListener( Closure listener ) {
        def entry = listenersMap.get(listener)
        if ( !entry ) { log.warn "No listener registered for: $listener"; return }
        (jobsMap as IMap).removeEntryListener(entry)
    }



}
