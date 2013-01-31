import com.hazelcast.core.Hazelcast
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IList
import circo.model.TaskResult
import spock.lang.Specification

import java.util.concurrent.BlockingQueue
/**
 *
 *  @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class HazelcastTest extends Specification {

    def cleanup () {
        println "** Cleanup"
        Hazelcast.shutdownAll()
    }

    def testHazelcast() {
        when:
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(null);
        BlockingQueue queue = hz.getQueue("tasks");
        queue.add( 'Hola' )

        then:
        queue.peek() != null
    }


    def testHazelcastList() {
        when:
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(null);
        IList list  = hz.getList('somelist')

        list.add('Hola')
        list.add(0,'Ciao')
        list.remove(0)

        then:
        list.size() == 1
        list.get(0) == 'Hola'


    }


    def testUpdate() {

        when:
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(null);
        TaskResult item = new TaskResult(exitCode: 1)

        IList<TaskResult> list  = hz.getList('somelist')
        list.add(item)

        list.get(0).exitCode = 2

        then:
        item.exitCode == 1


    }
}

