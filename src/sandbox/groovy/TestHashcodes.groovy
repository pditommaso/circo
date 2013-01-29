

import com.google.common.base.Optional
import spock.lang.Specification

/**
 *
 *  @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class TestHashcodes extends Specification {

    def testHash() {
        when:
        def x = Optional.of(Integer.valueOf(1))
        def y = Optional.of(Integer.valueOf(1))

        then:
        x == y

    }
}
