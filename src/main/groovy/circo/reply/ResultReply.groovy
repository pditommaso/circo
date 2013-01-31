package circo.reply

import circo.model.TaskResult
import circo.util.SerializeId
import groovy.transform.EqualsAndHashCode
import groovy.transform.InheritConstructors
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@SerializeId
@InheritConstructors
@EqualsAndHashCode(includes = 'ticket,result')
class ResultReply extends AbstractReply {

    /**
     * A {@code TaskResult} instance
     */
    TaskResult result

    ResultReply( UUID ticket, TaskResult result ) {
        super(ticket)
        this.result = result
    }

    String toString() {
        "${this.class.simpleName}(ticket=${ticket}, result=${result})"
    }

}
