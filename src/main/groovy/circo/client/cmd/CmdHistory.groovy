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

package circo.client.cmd
import circo.client.ClientApp
import circo.util.CircoHelper
import com.beust.jcommander.Parameters
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Parameters(commandNames = 'history', commandDescription = 'Display the history of executed commands')
class CmdHistory extends AbstractCommand {

    @Override
    void execute(ClientApp client) throws IllegalArgumentException {

        def len = client.console.history.size().toString().length()+1
        client.console.history.each {
            println "${CircoHelper.fmt(it.index(),len)}  ${it.value()}"
        }

    }
}
