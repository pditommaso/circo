import com.hazelcast.config.Config
import com.hazelcast.config.MultiMapConfig
import com.hazelcast.core.Hazelcast
import com.hazelcast.core.MapStore
import com.hazelcast.core.MultiMap
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

/**
 *
 *  @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */


class DummyMultiMap implements MapStore<Integer, String> {

    @Override
    void store(Integer key, String value) {
        println "store: $key = $value"
    }

    @Override
    void storeAll(Map<Integer, String> map) {
        println "storeAll: ${map}"
    }

    @Override
    void delete(Integer key) {
    }

    @Override
    void deleteAll(Collection<Integer> keys) {
    }

    @Override
    String load(Integer key) {
        return null  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    Map<Integer, String> loadAll(Collection<Integer> keys) {
        return null  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    Set<Integer> loadAllKeys() {
        return null  //To change body of implemented methods use File | Settings | File Templates.
    }
}

def conf = """
    <hazelcast>
      <multimap name='hola'>
        <multimap-store enabled="true" class-name='DummyMultiMap' />
      </multimap>
    </hazelcast>
    """

def cfg = new Config()
cfg.addMultiMapConfig( new MultiMapConfig()  )


def hz = Hazelcast.newHazelcastInstance( cfg )

MultiMap<Integer, String> map = hz.getMultiMap('hola')

println "Setting first value"
map.put(1, 'a')

println "Setting second value"
map.put(1, 'b')

println "Done"