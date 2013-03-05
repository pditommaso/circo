/*
 * Copyright (c) 2012, the authors.
 *
 *    This file is part of 'Circo'.
 *
 *    Circo is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation, either version 3 of the License, or
 *    (at your option) any later version.
 *
 *    Circo is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with Circo.  If not, see <http://www.gnu.org/licenses/>.
 */

package circo.client

import com.beust.jcommander.Parameter
import com.beust.jcommander.Parameters
import groovy.transform.ToString
import groovy.util.logging.Slf4j

/**
 * Implements to kill command to abort on-going jobs
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */

@Slf4j
@ToString(includeNames = true, includePackage = false)
@Parameters(commandNames = 'kill', commandDescription = 'Terminates a running job')
class CmdKill extends AbstractCommand {

    @Parameter(description = 'List of the jobs or tasks to kill')
    List<String> itemsToKill

    @Parameter(names=['-t','--task'])
    boolean tasks

    @Parameter(names='--ALL', description = 'Kill all pending and running jobs')
    boolean all


    @Override
    void execute(ClientApp client) throws IllegalArgumentException {

        if( !itemsToKill && !all ) {
            throw new IllegalArgumentException('Specify the job to kill by entering its id')
        }

        client.send(this)?.printMessages()

    }
}
