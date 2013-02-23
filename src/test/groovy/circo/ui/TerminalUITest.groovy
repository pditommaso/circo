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

package circo.ui
import akka.actor.Props
import akka.actor.UntypedActorFactory
import akka.testkit.JavaTestKit
import akka.testkit.TestActorRef
import circo.model.NodeData
import circo.model.TaskId
import test.ActorSpecification
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class TerminalUITest extends ActorSpecification {


    def "test renderScreen"   () {

        setup:
        final Props props = new Props( { new TerminalUIMock(dataStore, 11) } as UntypedActorFactory );
        final TestActorRef<TerminalUIMock> master = TestActorRef.create(system, props, "Terminal")

        def nodeData = new NodeData( id: 11 )
        def w1 = nodeData.createWorkerData( new JavaTestKit(system).getRef() )
        def w2 = nodeData.createWorkerData( new JavaTestKit(system).getRef() )
        def w3 = nodeData.createWorkerData( new JavaTestKit(system).getRef() )
        def w4 = nodeData.createWorkerData( new JavaTestKit(system).getRef() )

        nodeData.processed = 32
        nodeData.failed = 6

        w1.with {
            currentTaskId = TaskId.of('123')
            processed = 123
            failed = 3
        }

        w2.with {
            currentTaskId = TaskId.of('555')
            processed = 943
            failed = 76
        }

        w3.with {
            currentTaskId = TaskId.of('6577')
            processed = 7843
            failed = 111
        }

        dataStore.storeNodeData(nodeData)

        when:
        def screen = master.underlyingActor().renderScreen()

        print(screen)

        then:
        noExceptionThrown()

    }





}
