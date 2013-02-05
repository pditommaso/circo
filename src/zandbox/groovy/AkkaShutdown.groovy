import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.Terminated
import akka.actor.UntypedActor
import com.typesafe.config.ConfigFactory
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

class SimpleActor extends UntypedActor {

    def void preStart() {
        println "Starting: " + self().path().toString()
        def actor = getContext().actorOf( new Props(Parent), 'parent' )
        getContext().watch(self())
    }

    def void postStop() {
        println "Stopping: " + self().path().toString()
        //getContext().system().shutdown()
    }

    @Override
    void onReceive(def message) throws Exception {
        println "-> $message"

        if ( message instanceof  String ) {
            //println ">> $message"
        }
        else if ( message instanceof  Terminated ) {
            println "* Terminated ! "

        }
    }
}

class Parent extends UntypedActor {

    def void preStart() {
        println "Starting: " + self().path().toString()
        getContext().actorOf( new Props(Child), 'child1' )
        getContext().actorOf( new Props(Child), 'child2' )
    }

    def void postStop() {
        println "Stopping: " + self().path().toString()
    }

    @Override
    void onReceive(Object o) throws Exception {

    }
}

class Child extends UntypedActor {

    def void preStart() {
        println "Starting: " + self().path().toString()
    }

    def void postStop() {
        println "Stopping: " + self().path().toString()
    }

    @Override
    void onReceive(Object o) throws Exception {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}


def system = ActorSystem.create('test', ConfigFactory.empty())
def root = system.actorOf(new Props(SimpleActor), 'root')
root.tell "hello", null

println "wait"
sleep 500

println "shutting down"
//root.tell( PoisonPill.instance, null )
system.shutdown()
println "done"
