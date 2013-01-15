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
import akka.actor.*
import akka.testkit.JavaTestKit
import akka.testkit.TestActorRef
import groovy.util.logging.Slf4j
import rush.data.WorkerProbe

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
class TestHelper {

    static Map<Class,Integer> actorNames = new HashMap<>()

    static private String nextName( Class clazz ) {
        synchronized (actorNames) {
            Integer count = actorNames.get(clazz)
            count = (count == null ) ? new Integer(0) : count+1
            actorNames.put(clazz,count)
            return clazz.simpleName + count
        }
    }

    static Address addr( String address = null, int port = 2551 ) {
        address ? new Address('akka','default', address, port ) : new Address('akka','default')
    }

    static Address defaultAddr() { addr() }


    static JavaTestKit newProbe( ActorSystem system ) {
        new JavaTestKit(system)
    }

    static WorkerProbe newWorkerProbe( ActorSystem system) {
        return WorkerProbe.create(system)
    }

    static Map<Class,MetaClass> defaultMetaClass = new HashMap<>()

    static <T extends UntypedActor> TestActorRefEx<T> newTestActor( ActorSystem system, Class<T> clazz, Closure factory = null ) {
        assert clazz

        final thisProbe = newProbe(system)
        final actorName = nextName(clazz)
        final probeRef = thisProbe.getRef()
        log.debug "TestActor: '$actorName' using probe: ${thisProbe.getRef()}"
        clazz.metaClass.getSelf = { probeRef }

        def createAndInjectProbe
        if( !factory ) {

            createAndInjectProbe = {
                def instance = clazz.newInstance()
                instance .metaClass.getSelf = { thisProbe.getRef() }
                return instance

            } as UntypedActorFactory

        }
        else {

            createAndInjectProbe = {
                def instance = factory.call()
                instance .metaClass.getSelf = { thisProbe.getRef() }
                return instance

            } as UntypedActorFactory

        }

        def target = TestActorRef.create(system, new Props(createAndInjectProbe), actorName )
        def result = new TestActorRefEx<T>(target,thisProbe)

        return result
    }

    @Deprecated
    static <T extends UntypedActor> T newActor( ActorSystem system, Class<T> clazz, Closure factory = null ) {
        def test = newTestActor(system,clazz,factory)
        return test.underlyingActor()
    }

    @Deprecated
    static <T extends UntypedActor> T newProbeOf( ActorSystem system, Class<T> clazz, Closure factory = null ) {
        def testActorRef = newTestActor(system,clazz,factory)
        def actor = testActorRef.underlyingActor()
        def probe = newProbe(system)
        actor.metaClass.getSelf = { probe.getRef() }
        actor.metaClass.getProbe = { probe }
        actor.metaClass.tell = { def message ->  testActorRef.tell(message) }
        actor.metaClass.tell = { def message, ActorRef sender -> testActorRef.tell(message, sender) }

        return actor
    }


    static class TestActorRefEx<T>  {

        @Delegate
        TestActorRef<T> target

        JavaTestKit probe

        TestActorRefEx( TestActorRef<T> target, JavaTestKit probe ) {
            this.target = target
            this.probe = probe
        }

        def T getActor() { target.underlyingActor() }

        def JavaTestKit getProbe() { probe }

    }

    static void updateJavaLibPath() {

        def libPath = new File( System.properties['user.home'] as String, 'workspace/rush/lib' )
        def javaLibPath = System.properties['java.library.path']
        javaLibPath += ":" + libPath.absolutePath
        log.debug "Setting 'java.lib.path' to ${javaLibPath}"
        System.setProperty('java.library.path', javaLibPath)

    }



}
