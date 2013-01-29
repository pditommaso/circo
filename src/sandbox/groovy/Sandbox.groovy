
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.UntypedActorFactory
import akka.transactor.UntypedTransactor
import com.google.common.base.Optional
import com.typesafe.config.ConfigFactory
import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString
import circo.util.AkkaHelper
import org.apache.commons.lang.StringUtils
import scala.concurrent.stm.Ref.View as RefView
import scala.concurrent.stm.japi.STM
import spock.lang.Specification

import java.util.concurrent.TimeUnit
/**
 *
 *  @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class Sandbox extends Specification {

    def testLocalHost() {

        when:

        println InetAddress.getLocalHost().hostAddress

        then:
        true



    }

    def testHash() {
        when:
        def x = Optional.of(Integer.valueOf(1))
        def y = Optional.of(Integer.valueOf(1))

        then:
        x == y

    }


    def testWaitFor() {

        when:
        Process process = new ProcessBuilder().command('bash', '-c', 'sleep 2; exit 99').start()

        then:
        process.waitFor() == 99
    }

    def testWaitForOrKill() {

        when:
        Process process = new ProcessBuilder().command('bash', '-c', 'sleep 5; exit 99').start()

        then:
        process.waitForOrKill(2000)
        process.exitValue() == 99

    }


    def testTimeUnit() {

        expect:
        TimeUnit.MINUTES.toMillis(5) == 5 * 60 * 1000
    }

    def testThreadInterruption() {

        when:

        def now = System.currentTimeMillis()
        def thread = new Thread() {

           boolean terminate

            def void run() {
                while( !terminate && System.currentTimeMillis() - now < 5000 ) {
                    try { Thread.sleep(100);  }  catch( InterruptedException e ) { println "** interrupted ** " }
                }

            }


        }
        thread.start()

        Thread.start { Thread.sleep(500); thread.terminate = true;  println "Interruped" }

        thread.join()
        def delta = System.currentTimeMillis() - now

        then:
        delta < 1000


    }

    @ToString
    @EqualsAndHashCode
    static class MyData implements Serializable {

        Integer value = new Integer(0)

        List<Integer> list = new ArrayList<>()

        def void inc() {
            value += 1
            list << value
            println "** inc ** ==> ${this}"
        }
    }


    RefView<MyData> data = STM.<MyData>newRef( new MyData() )

    class Counter extends UntypedTransactor {



        def void preStart() {  println 'start' }

        def void postStop() { println "stop: ${data.get()}" }

//        SupervisorStrategy supervisorStrategy() {
//
//        }

        public void atomically(Object message) {
            if (message == "inc") {
                data.get().inc()
            }
            else if( message == 'boom' ) {
                data.get().inc()
                throw new IllegalStateException('Boom!')
            }
        }

        @Override public boolean normally(Object message) {
            if ( message == 'get' ) {
                Counter.this.getSender().tell(data.get(), getSelf());
                return true;
            }
            else {
                return false
            }

        }
    }


    def testTransactor() {

        setup:
        def system = ActorSystem.create('default', ConfigFactory.empty())
        //TestActorRef<Counter> counter = akka.testkit.TestActorRef.create(system, new Props(Counter), 'c' )
        //def probe = new JavaTestKit(system)


        def actor = system.actorOf(new Props({ new Counter() } as UntypedActorFactory ), 'c' )


        when:
        actor.tell ( "inc" )
        actor.tell ( "inc" )
        actor.tell ( "boom" )

        def result = AkkaHelper.ask( actor, 'get', 1000 )
        println ">> ${result}"


        then:
        true

        cleanup:
        system.shutdown()

    }


    def testDiff() {

        when:
        println StringUtils.indexOfDifference('alpha beta gamma delta', 'alpha beta x gamma delta')
        println StringUtils.difference('alpha beta gamma delta'.substring(11), 'alpha beta gamma delta'.substring(11))

        then:
        true

    }
}
