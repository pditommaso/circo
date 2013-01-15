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

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.testkit.JavaTestKit

import static test.TestHelper.newProbe

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class WorkerProbe extends WorkerRef {

    def JavaTestKit probe

    private WorkerProbe(ActorRef actor) {
        super(actor)
    }

    static create( ActorSystem system ) {

        def probe = newProbe(system)
        def result = new WorkerProbe(probe.getRef())
        result.probe = probe
        return result
    }
}
