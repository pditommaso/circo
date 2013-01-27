package circo.messages

import spock.lang.Specification

/**
 *
 *  @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class JobEntryTest extends Specification {

    def testEqualsAndHashCode() {

        when:
        def val1 = new JobEntry(new JobId('1'), new JobReq(script: 'Hola'))
        def val2 = new JobEntry(new JobId('1'), new JobReq(script: 'Hola'))

        then:
        val1 == val2
        val1.hashCode() == val2.hashCode()

    }

    def 'tes set status' () {

        when:
        def timestamp = System.currentTimeMillis()
        def job1 = JobEntry.create('1')

        def job2 = JobEntry.create('2')
        job2.status = JobStatus.RUNNING

        def job3 = JobEntry.create('3')
        job3.status = JobStatus.RUNNING
        job3.status = JobStatus.COMPLETE

        then:
        job1.creationTime >= timestamp
        job1.launchTime == 0
        job1.completionTime == 0

        job2.status == JobStatus.RUNNING
        job2.creationTime >= timestamp
        job2.launchTime >= timestamp
        job2.completionTime == 0

        job3.status == JobStatus.COMPLETE
        job3.creationTime >= timestamp
        job3.launchTime >= timestamp
        job3.completionTime >= timestamp
        job3.completionTime <= System.currentTimeMillis()

    }


    def testIsValidExitCode () {

        when:
        def cmd = new JobEntry(new JobId('1'), new JobReq( validExitCode: valid instanceof List ? valid : [valid] ))

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
        def job1 = JobEntry.create(1)
        def job2 = JobEntry.create(2)
        def job3 = JobEntry.create(3)
        def job4 = JobEntry.create(4) { it.req.validExitCode = [ 1, 2 ] }

        when:
        job1.result = new JobResult(exitCode: 0)
        job2.result = new JobResult(exitCode: 0, cancelled: true)
        job3.result = new JobResult(exitCode: 1 )
        job4.result = new JobResult(exitCode: 1 )

        then:
        job1.isSuccess()
        !job2.isSuccess()
        job3.isSuccess() == false
        job4.isSuccess() == true

    }

    def 'test isFailed' () {
        setup:
        def job1 = JobEntry.create(1)
        def job2 = JobEntry.create(2)
        def job3 = JobEntry.create(3)
        def job4 = JobEntry.create(4) { it.req.validExitCode = [ 1, 2 ] }

        when:
        job1.result = new JobResult(exitCode: 0)
        job2.result = new JobResult(exitCode: 0, cancelled: true)
        job3.result = new JobResult(exitCode: 1 )
        job4.result = new JobResult(exitCode: 1 )

        then:
        !job1.isFailed()
        !job2.isFailed()
        job3.isFailed()
        !job4.isFailed()
    }

    def 'test isCancelled' () {
        setup:
        def job1 = JobEntry.create(1)
        def job2 = JobEntry.create(2)
        def job3 = JobEntry.create(3)
        def job4 = JobEntry.create(4) { it.req.validExitCode = [ 1, 2 ] }

        when:
        job1.result = new JobResult(exitCode: 0)
        job2.result = new JobResult(exitCode: 0, cancelled: true)
        job3.result = new JobResult(exitCode: 1 )
        job4.result = new JobResult(exitCode: 1 )

        then:
        !job1.cancelled
        job2.cancelled
        !job3.cancelled
        !job4.cancelled
    }


    def 'test setResult ' () {


        when:
        // this JOB is OK
        def job1 = JobEntry.create(1)
        job1.result = new JobResult(exitCode: 0)

        def job2 = JobEntry.create(2)
        job2.result = new JobResult(exitCode: 1)

        def job3 = JobEntry.create(3)
        job3.result = new JobResult(exitCode: 1, cancelled: true)

        def job4 = JobEntry.create(4) { JobEntry it -> it.req.maxAttempts = 5; it.attempts = 5 }
        job4.result = new JobResult( exitCode: 1 )

        def job5 = JobEntry.create(5) { JobEntry it -> it.req.maxAttempts = 5; it.attempts = 5 }
        job5.result = new JobResult( exitCode: 1, cancelled: true )

        def job6 = JobEntry.create(2)
        job6.result = new JobResult(exitCode: 0, failure: new Exception('Error'))

        then:
        // this is OK
        job1.status == JobStatus.COMPLETE
        job1.isSuccess()
        !job1.isFailed()
        !job1.cancelled
        !job1.retryIsRequired()

        // terminated with error BUT not completed because it can be retried
        job2.status != JobStatus.COMPLETE
        !job2.isSuccess()
        job2.isFailed()
        !job2.cancelled
        job2.retryIsRequired()

        // cancelled, so NOT completed, NOT failed, it can be retried
        job3.status != JobStatus.COMPLETE
        !job3.isSuccess()
        !job3.isFailed()
        job3.isCancelled()
        job3.retryIsRequired()

        // error result - and - max number of attempts met,
        // job FAILED
        job4.status == JobStatus.FAILED
        !job4.isSuccess()
        job4.isFailed()
        !job4.isCancelled()
        !job4.retryIsRequired()

        // job at last attempt - BUT CANCELLED
        // so it can re retried
        job5.status != JobStatus.FAILED
        job5.status != JobStatus.COMPLETE
        !job5.isSuccess()
        !job5.isFailed()
        job5.isCancelled()
        job5.retryIsRequired()

        // job with exitcode == 0 BUT failure not null
        job6.status == JobStatus.FAILED
        !job6.isSuccess()
        job6.isFailed()
        !job6.isCancelled()
        job6.retryIsRequired()

    }


}
