package circo.client

import com.beust.jcommander.Parameter
import com.beust.jcommander.Parameters

/**
 * Get a job result from teh cache
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Parameters(commandNames='get', commandDescription = 'Retrieve the result to computed jobs')
class CmdGet extends AbstractCommand {

    @Parameter(description = 'List of job ids for which retrieve the result')
    List<String> listOfIds


    @Override
    void execute(ClientApp client) throws IllegalArgumentException {

        if ( !listOfIds ) {
            throw new IllegalArgumentException('Please specify at least one job id')
        }

        def result = client.send(this)

        result.printMessages()

    }


    def int expectedReplies() {
        listOfIds?.size() ?: 0
    }


}
