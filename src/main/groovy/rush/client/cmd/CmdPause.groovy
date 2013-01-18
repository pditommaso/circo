/*
 * Copyright (c) 2012, the authors.
 *
 *    This file is part of RUSH.
 *
 *    Moke is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation, either version 3 of the License, or
 *    (at your option) any later version.
 *
 *    Moke is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with RUSH.  If not, see <http://www.gnu.org/licenses/>.
 */

package rush.client.cmd

import com.beust.jcommander.Parameter
import com.beust.jcommander.Parameters
import rush.client.ClientApp

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Parameters(commandNames='stop', commandDescription = 'Stop the current ' )
class CmdPause extends AbstractCommand {

    @Parameter(names='--HARD', description = )
    boolean hard

    @Override
    void execute(ClientApp client) throws IllegalArgumentException {
    }
}
