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
import akka.actor.ActorSystem
import akka.testkit.JavaTestKit
import com.typesafe.config.ConfigFactory
import circo.messages.JobId
import spock.lang.Specification
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class WorkerDataTest extends Specification  {

    ActorSystem system

    def setup() {
        system = ActorSystem.create('default', ConfigFactory.empty())
    }

    def cleanup() {
        system.shutdown()
    }

    def 'test equalsAndHashCode' () {

        when:
        def ref = new WorkerRef(new JavaTestKit(system).getRef())
        def worker1 = WorkerData.of(ref) { it.currentJobId=JobId.of(74893) }
        def worker2 = WorkerData.of(ref) { it.currentJobId=JobId.of(74893) }
        def worker3 = WorkerData.of(ref) { it.currentJobId=JobId.of(12345) }

        then:
        worker1 == worker2
        worker1 != worker3

        worker1.hashCode() == worker2.hashCode()
        worker1.hashCode() != worker3.hashCode()


    }
}
