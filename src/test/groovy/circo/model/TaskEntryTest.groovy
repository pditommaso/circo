package circo.model

import spock.lang.Specification
/**
 *
 *  @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class TaskEntryTest extends Specification {

    def testEqualsAndHashCode() {

        when:
        def val1 = new TaskEntry(new TaskId('1'), new TaskReq(script: 'Hola'))
        def val2 = new TaskEntry(new TaskId('1'), new TaskReq(script: 'Hola'))

        then:
        val1 == val2
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
        job3.status = TaskStatus.COMPLETE

        then:
        job1.creationTime >= timestamp
        job1.launchTime == 0
        job1.completionTime == 0

        job2.status == TaskStatus.RUNNING
        job2.creationTime >= timestamp
        job2.launchTime >= timestamp
        job2.completionTime == 0

        job3.status == TaskStatus.COMPLETE
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

        def job2 = TaskEntry.create(2)
        job2.result = new TaskResult(exitCode: 1)

        def job3 = TaskEntry.create(3)
        job3.result = new TaskResult(exitCode: 1, cancelled: true)

        def job4 = TaskEntry.create(4) { TaskEntry it -> it.req.maxAttempts = 5; it.attempts = 5 }
        job4.result = new TaskResult( exitCode: 1 )

        def job5 = TaskEntry.create(5) { TaskEntry it -> it.req.maxAttempts = 5; it.attempts = 5 }
        job5.result = new TaskResult( exitCode: 1, cancelled: true )

        def job6 = TaskEntry.create(2)
        job6.result = new TaskResult(exitCode: 0, failure: new Exception('Error'))

        then:
        // this is OK
        job1.status == TaskStatus.COMPLETE
        job1.isSuccess()
        !job1.isFailed()
        !job1.cancelled
        !job1.retryIsRequired()
        job1.isDone()

        // terminated with error BUT not completed because it can be retried
        job2.status != TaskStatus.COMPLETE
        !job2.isSuccess()
        job2.isFailed()
        !job2.cancelled
        job2.retryIsRequired()
        job2.isDone()

        // cancelled, so NOT completed, NOT failed, it can be retried
        job3.status != TaskStatus.COMPLETE
        !job3.isSuccess()
        !job3.isFailed()
        job3.isCancelled()
        job3.retryIsRequired()
        job3.isDone()

        // error result - and - max number of attempts met,
        // job FAILED
        job4.status == TaskStatus.FAILED
        !job4.isSuccess()
        job4.isFailed()
        !job4.isCancelled()
        !job4.retryIsRequired()
        job4.isDone()

        // job at last attempt - BUT CANCELLED
        // so it can re retried
        job5.status != TaskStatus.FAILED
        job5.status != TaskStatus.COMPLETE
        !job5.isSuccess()
        !job5.isFailed()
        job5.isCancelled()
        job5.retryIsRequired()
        job5.isDone()

        // job with exit code == 0 BUT failure not null
        job6.status == TaskStatus.FAILED
        !job6.isSuccess()
        job6.isFailed()
        !job6.isCancelled()
        job6.retryIsRequired()
        job6.isDone()

    }

    def 'test toString' () {

        when:
        def job = new TaskEntry('1','echo x')
        job.status = TaskStatus.RUNNING

        then:
        job.toString() == "TaskEntry(id=1, status=RUNNING, hasResult=false, exitCode=-, failure=-, cancelled=-, attemptTimes=0, cancelledTimes=0 )"

    }


}
