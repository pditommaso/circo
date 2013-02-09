import java.util.concurrent.CountDownLatch

/**
 *
 *  @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */


CountDownLatch barrier = new CountDownLatch(1)

Thread.start {  sleep (500); barrier.countDown() }

println "waiting.."
barrier.await()
println "passed"

println 'passthrough..'
barrier.await()
barrier.countDown()
barrier.countDown()
barrier.await()

println('done')

