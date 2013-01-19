/*
 * Copyright (c) 2012, the authors.
 *
 *    This file is part of Circo.
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

import com.beust.jcommander.JCommander
import com.beust.jcommander.Parameter
import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString
import groovy.transform.TupleConstructor
import circo.client.ClientApp

/**
 * Base class for all commands
 *
 *  @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@ToString(includes = 'ticket', includePackage = false, includeNames = true)
public abstract class AbstractCommand implements Serializable {

    /**
     * Define the 'help' command line option for all subclasses command
     */
    @Parameter(names=['-h','--help'], description = 'Show this help', help= true)
    boolean help

    /**
     * Request identifier, each request submitted to teh server has a Unique Universal identifier
     */
    String ticket

    /**
     * Implements the client interaction
     *
     * @param client The {@code ClientApp} instance
     * @throws IllegalArgumentException When the provided command argument are not valid
     */
    def abstract void execute( ClientApp client ) throws IllegalArgumentException

}


@TupleConstructor
class AppOptionsException extends Exception {

    CommandParser parsedCommand

}

@TupleConstructor
class AppHelpException extends Exception {

    CommandParser parsedCommand

}


/**
 * Hold the Client application main options
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@ToString(includePackage = false)
@EqualsAndHashCode
class AppOptions {

    @Parameter(names=['-h','--help'], help=true)
    boolean help


    def AppOptions() { }

    def AppOptions( AppOptions that ) {

        this.help = that.help

    }

}

/**
 * Creates the JCommander command structure
 */
class AppCommandsFactory {

    static def JCommander create() {

        create(new AppOptions())

    }


    static def JCommander create( AppOptions appOptions ) {

        def cmdParser = new JCommander(appOptions)
        cmdParser.addCommand( new CmdSub() )
        cmdParser.addCommand( new CmdNode() )
        cmdParser.addCommand( new CmdStat() )
        cmdParser.addCommand( new CmdClear() )

        return cmdParser
    }


}
