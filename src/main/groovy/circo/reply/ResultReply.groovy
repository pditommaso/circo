package circo.reply

import circo.messages.JobResult
import groovy.transform.InheritConstructors
import groovy.transform.ToString

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@InheritConstructors
@ToString(includePackage = false)
class ResultReply extends AbstractReply {

    /**
     * A {@code JobResult} instance
     */
    JobResult result

}
