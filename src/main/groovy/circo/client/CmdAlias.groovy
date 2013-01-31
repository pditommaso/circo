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

import com.beust.jcommander.DynamicParameter
import com.beust.jcommander.Parameter

/**
 * TODO
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class CmdAlias extends AbstractCommand {

    Map<String,String> set


    @DynamicParameter(names=['-d','--def'], description = 'Define a new alias name')
    Map<String,String> fDef = new HashMap<>()

    @Parameter(names=['-u','--undef'], description = 'Remove the specified value from the current context')
    List<String> fUndef


    @Override
    void execute(ClientApp client) throws IllegalArgumentException {

    }
}
