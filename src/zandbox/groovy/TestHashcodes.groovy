import com.google.common.base.Optional

/**
 *
 *  @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */

def x = Optional.of(Integer.valueOf(1))
def y = Optional.of(Integer.valueOf(1))

assert x == y