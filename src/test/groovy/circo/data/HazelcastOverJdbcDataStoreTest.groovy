/*
 * Copyright (c) 2012, the authors.
 *
 *    This file is part of 'Circo'.
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

import circo.data.sql.JdbcDataSourceFactory
import com.hazelcast.config.Config
import com.hazelcast.core.Hazelcast
import com.hazelcast.core.HazelcastInstance
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class HazelcastOverJdbcDataStoreTest extends DataStoreTest {


    def void setup() {

        def ds = JdbcDataSourceFactory.create('jdbc:h2:mem:Circo', ['acquireIncrement':1, 'partitionCount':1, 'minConnectionsPerPartition':1])

        Config cfg = new Config()
        HazelcastDataStore.configureAllMapStores(cfg, true)
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg)

        store = new HazelcastDataStore(hz,ds)

    }

    def void cleanup() {
        store?.shutdown()
    }

}
