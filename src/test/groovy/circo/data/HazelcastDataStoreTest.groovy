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
import circo.model.AddressRef
import circo.model.NodeData
import circo.model.NodeStatus
import com.hazelcast.core.Hazelcast
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class HazelcastDataStoreTest extends AbstractDataStoreTest {

    def void setup() {
        store = new HazelcastDataStore()
    }

    def void cleanup() {
        Hazelcast.shutdownAll()
    }

    def 'test getNodePartition' () {

        setup:
        def addr = new AddressRef(InetAddress.localHost.getHostAddress(), 5701)
        def theNode = new NodeData(status: NodeStatus.ALIVE, storeMemberId: addr)
        store.saveNode(theNode)

        expect:
        store.getPartitionNode( 1 )  == theNode
        store.getPartitionNode( 2 )  == theNode


    }

    def 'test partitionNodes' () {
        setup:
        def addr = new AddressRef(InetAddress.localHost.getHostAddress(), 5701)
        def theNode = new NodeData(status: NodeStatus.ALIVE, storeMemberId: addr)
        store.saveNode(theNode)

        when:
        def map = [:]
        store.partitionNodes([1,2,3]) { Object entry, NodeData node -> map[entry]=node }

        then:
        map[1] == theNode
        map[2] == theNode
        map[3] == theNode
        map.size() == 3

    }

}
