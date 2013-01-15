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

import akka.actor.ActorSystem
import akka.actor.Address
import akka.actor.RootActorPath
import akka.testkit.JavaTestKit
import com.typesafe.config.ConfigFactory
import spock.lang.Specification

import static test.TestHelper.addr

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class WorkerRefTest extends Specification {

    ActorSystem system
    Address selfAddress

    def setup() {
        system = ActorSystem.create('default', ConfigFactory.empty())
        selfAddress = addr('1.1.1.1')
    }

    def cleanup () {
        system.shutdown()
    }


    def "test tell" () {

        setup:
        def actor = new JavaTestKit(system)
        def worker = new WorkerRef(actor.getRef())

        when:
        worker.tell( 'Hola' )

        then:
        actor.expectMsgEquals('Hola')

    }

    def "test address" () {

        setup:
        WorkerRef.init(system,selfAddress)
        def actor = new JavaTestKit(system)
        def worker = new WorkerRef(actor.getRef())

        expect:
        worker.address() == addr('1.1.1.1')
        worker.address().toString() == 'akka://default@1.1.1.1:2551'

    }

    def "test createByPath" () {

        setup:
        WorkerRef.init(system,selfAddress)
        def worker = new WorkerRef(new RootActorPath( addr('2.2.2.2',2552), '/user/MyActor'))

        expect:
        worker.address().toString() == 'akka://default@2.2.2.2:2552'
        worker.toString() == 'WorkerRef(akka://default@2.2.2.2:2552/user/MyActor)'

    }

    def "test equalsAndHashCode" () {

        when:
        def probe1 = new JavaTestKit(system).getRef()
        def probe2 = new JavaTestKit(system).getRef()
        def w1 = new WorkerRef(probe1)
        def w2 = new WorkerRef(probe2)
        def w3 = new WorkerRef(probe2)

        then:
        w1 != w2
        w2 == w3
        w1.hashCode() != w2.hashCode()
        w2.hashCode() == w3.hashCode()

    }

    def "test workerMock parse" () {

        expect:
        WorkerRefMock.parse('/some/path') == [ new Address('akka','default'), '/some/path' ]
        WorkerRefMock.parse('akka://TestSystem/some/path') == [ new Address('akka','TestSystem'), '/some/path' ]
        WorkerRefMock.parse('akka://Sys@1.1.1.1/some/path') == [ new Address('akka','Sys','1.1.1.1',2551), '/some/path' ]
        WorkerRefMock.parse('akka://Sys@1.1.1.1:555/some/path') == [ new Address('akka','Sys','1.1.1.1',555), '/some/path' ]

    }

    def "test WorkerMock path" () {
        when:
        def mock = new WorkerRefMock('akka://def@1.1.1.1/user/actor')

        then:
        mock.path == 'akka://def@1.1.1.1:2551/user/actor'
        mock.address() == new Address('akka','def','1.1.1.1',2551)
    }
}
