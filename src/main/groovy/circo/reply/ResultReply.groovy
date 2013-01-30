package circo.reply
import circo.messages.JobResult
import groovy.transform.EqualsAndHashCode
import groovy.transform.InheritConstructors
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@InheritConstructors
@EqualsAndHashCode(includes = 'ticket,result')
class ResultReply extends AbstractReply {

    /**
     * A {@code JobResult} instance
     */
    JobResult result

    ResultReply( UUID ticket, JobResult result ) {
        super(ticket)
        this.result = result
    }

    String toString() {
        "${this.class.simpleName}(ticket=${ticket}, result=${result})"
    }

}
