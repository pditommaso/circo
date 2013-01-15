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

package test

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import rush.data.DataStore
import rush.data.LocalDataStore
import spock.lang.Specification

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
abstract class ActorSpecification extends Specification {

    static ActorSystem system

    static DataStore dataStore

    def void setup () {
        system = ActorSystem.create( 'default', ConfigFactory.empty() )
        dataStore = new LocalDataStore()
    }

    def void cleanup () {
        system.shutdown()
    }

}
