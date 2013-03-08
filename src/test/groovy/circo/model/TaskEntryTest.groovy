package circo.model

import org.apache.commons.lang.SerializationUtils
import spock.lang.Specification
/**
 *
 *  @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class TaskEntryTest extends Specification {

    def 'test EqualsAndHashCode'() {

        when:
        def val1 = new TaskEntry(new TaskId('1'), new TaskReq(script: 'Hola'))
        def val2 = SerializationUtils.clone( val1 )

        then:
        val1 == val2
        val1.equals( val2 )
        val1.hashCode() == val2.hashCode()

    }

    def 'tes set status' () {

        when:
        def timestamp = System.currentTimeMillis()
        def job1 = TaskEntry.create('1')

        def job2 = TaskEntry.create('2')
        job2.status = TaskStatus.RUNNING

        def job3 = TaskEntry.create('3')
        job3.status = TaskStatus.RUNNING
        job3.status = TaskStatus.TERMINATED

        then:
        job1.creationTime >= timestamp
        job1.launchTime == 0
        job1.completionTime == 0

        job2.status == TaskStatus.RUNNING
        job2.creationTime >= timestamp
        job2.launchTime >= timestamp
        job2.completionTime == 0

        job3.status == TaskStatus.TERMINATED
        job3.creationTime >= timestamp
        job3.launchTime >= timestamp
        job3.completionTime >= timestamp
        job3.completionTime <= System.currentTimeMillis()

    }


    def testIsValidExitCode () {

        when:
        def cmd = new TaskEntry(new TaskId('1'), new TaskReq( validExitCode: valid instanceof List ? valid : [valid] ))

        then:
        cmd.isValidExitCode(code) == result

        where:
        valid   | code    | result
        0       | 0       | true
        0       | 1       | false
        0       | 99      | false
        99      | 0       | false
        99      | 1       | false
        99      | 99      | true
        [0,1]   | 0       | true
        [0,1]   | 1       | true
        [0,1]   | 2       | false
    }


    def 'test isSuccess' () {

        setup:
        def job1 = TaskEntry.create(1)
        def job2 = TaskEntry.create(2)
        def job3 = TaskEntry.create(3)
        def job4 = TaskEntry.create(4) { it.req.validExitCode = [ 1, 2 ] }

        when:
        job1.result = new TaskResult(exitCode: 0)
        job2.result = new TaskResult(exitCode: 0, cancelled: true)
        job3.result = new TaskResult(exitCode: 1 )
        job4.result = new TaskResult(exitCode: 1 )

        then:
        job1.isSuccess()
        !job2.isSuccess()
        job3.isSuccess() == false
        job4.isSuccess() == true

    }

    def 'test isFailed' () {
        setup:
        def job1 = TaskEntry.create(1)
        def job2 = TaskEntry.create(2)
        def job3 = TaskEntry.create(3)
        def job4 = TaskEntry.create(4) { it.req.validExitCode = [ 1, 2 ] }

        when:
        job1.result = new TaskResult(exitCode: 0)
        job2.result = new TaskResult(exitCode: 0, cancelled: true)
        job3.result = new TaskResult(exitCode: 1 )
        job4.result = new TaskResult(exitCode: 1 )

        then:
        !job1.isFailed()
        !job2.isFailed()
        job3.isFailed()
        !job4.isFailed()
    }

    def 'test isCancelled' () {
        setup:
        def job1 = TaskEntry.create(1)
        def job2 = TaskEntry.create(2)
        def job3 = TaskEntry.create(3)
        def job4 = TaskEntry.create(4) { it.req.validExitCode = [ 1, 2 ] }

        when:
        job1.result = new TaskResult(exitCode: 0)
        job2.result = new TaskResult(exitCode: 0, cancelled: true)
        job3.result = new TaskResult(exitCode: 1 )
        job4.result = new TaskResult(exitCode: 1 )

        then:
        !job1.cancelled
        job2.cancelled
        !job3.cancelled
        !job4.cancelled
    }


    def 'test setResult ' () {

        when:
        // this JOB is OK
        def job1 = TaskEntry.create(1)
        job1.result = new TaskResult(exitCode: 0)

        then:
        // this is OK
        job1.status == TaskStatus.TERMINATED
        job1.isSuccess()
        !job1.isFailed()
        !job1.cancelled
        !job1.isRetryRequired()
        !job1.killed

        // unsuccessful - BUT - not terminated because it can be resubmitted
        when:
        def job2 = TaskEntry.create(2)
        job2.result = new TaskResult(exitCode: 1)

        then:
        !job2.terminated
        !job2.isSuccess()
        job2.isFailed()
        !job2.cancelled
        job2.isRetryRequired()
        !job2.killed

        // cancelled, so NOT completed, NOT failed, it can be retried
        when:
        def job3 = TaskEntry.create(3)
        job3.result = new TaskResult(exitCode: 1, cancelled: true)

        then:
        !job3.terminated
        !job3.isSuccess()
        !job3.isFailed()
        job3.isCancelled()
        job3.isRetryRequired()
        !job3.killed

        // error result - and - max number of attempts met,
        // job FAILED
        when:
        def job4 = TaskEntry.create(4) { TaskEntry it -> it.req.maxAttempts = 5; it.attemptsCount = 5 }
        job4.result = new TaskResult( exitCode: 1 )

        then:
        job4.terminated
        !job4.isSuccess()
        job4.isFailed()
        !job4.isCancelled()
        !job4.isRetryRequired()
        !job4.killed

        // job at last attempt - BUT CANCELLED
        // so it can re retried
        when:
        def job5 = TaskEntry.create(5) { TaskEntry it -> it.req.maxAttempts = 5; it.attemptsCount = 5 }
        job5.result = new TaskResult( exitCode: 1, cancelled: true )

        then:
        !job5.terminated
        !job5.isSuccess()
        !job5.isFailed()
        job5.isCancelled()
        job5.isRetryRequired()
        !job5.killed

        // job with exit code == 0 BUT failure not null
        when:
        def job6 = TaskEntry.create(6)
        job6.result = new TaskResult(exitCode: 0, failure: new Exception('Error'))

        then:
        !job6.terminated
        !job6.isSuccess()
        job6.isFailed()
        !job6.isCancelled()
        job6.isRetryRequired()
        !job6.killed


        // KILL a job
        when:
        def job7 = TaskEntry.create(7) { TaskEntry task -> task.killed = true; task.result = new TaskResult() }

        then:
        job7.killed
        job7.terminated
        !job7.cancelled
        !job7.retryRequired
        !job7.isRunning()
        !job7.isSuccess()
        !job7.isFailed()

    }

    def 'test sort ' () {
        given:
        def t1 = TaskEntry.create(1)
        def t2 = TaskEntry.create(2)
        def t3 = TaskEntry.create(3)
        def t4 = TaskEntry.create(4)
        def t5 = TaskEntry.create(5)
        def tasks = [ t3, t2, t4, t1, t5 ]

        expect:
        tasks.sort { TaskEntry it -> it.id } == [t1,t2,t3,t4,t5]
    }


}
