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

import circo.model.NodeData
import circo.model.NodeStatus
import groovy.sql.Sql
import spock.lang.Shared
import spock.lang.Specification
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class HzJdbcNodesMapStoreTest extends Specification {

    @Shared
    Sql sql

    def setup() {

        sql = Sql.newInstance('jdbc:h2:mem:Circo')

        def store = new HzJdbcNodesMapStore(sql: sql)
        store.dropTable()
        store.createTable()
    }


    def 'test store' () {
        setup:
        def nodeStore = new HzJdbcNodesMapStore(sql: sql)

        def node1 = new NodeData(id: 1, status: NodeStatus.ALIVE)
        def node2 = new NodeData(id: 2, status: NodeStatus.ALIVE)
        def node3 = new NodeData(id: 3, status: NodeStatus.DEAD)
        def node4 = new NodeData(id: 4)

        when:
        nodeStore.store( node1.id, node1 )
        nodeStore.store( node2.id, node2 )
        nodeStore.store( node3.id, node3 )
        nodeStore.store( node4.id, node4 )

        node4.status = NodeStatus.PAUSED
        nodeStore.store( node4.id, node4 )

        then:
        nodeStore.load(node1.id) == node1
        nodeStore.load(node2.id) == node2
        nodeStore.load(node2.id) != node3
        nodeStore.load(99) == null
        nodeStore.loadAllKeys().size() == 4

        nodeStore.loadAll() as Set == [ node1, node2, node3, node4 ] as Set

        nodeStore.load(node4.id).status == NodeStatus.PAUSED

    }

}
