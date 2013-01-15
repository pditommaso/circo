package rush.messages

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
}
