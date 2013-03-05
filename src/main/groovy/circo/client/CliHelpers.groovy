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

package circo.client

import com.beust.jcommander.JCommander
import com.beust.jcommander.Parameter
import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString
import groovy.transform.TupleConstructor
/**
 * Base class for all commands
 *
 *  @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@ToString(includes = 'ticket', includePackage = false, includeNames = true)
public abstract class AbstractCommand<R> implements Serializable {

    /**
     * Define the 'help' command line option for all subclasses command
     */
    @Parameter(names=['-h','--help'], description = 'Show this help', help= true)
    boolean help

    @Parameter(names=['--dump'], hidden = true)
    boolean dumpFlag

    /**
     * Request identifier, each request submitted to teh server has a Unique Universal identifier
     */
    UUID ticket

    /**
     * Implements the client interaction
     *
     * @param client The {@code ClientApp} instance
     * @throws IllegalArgumentException When the provided command argument are not valid
     */
    def abstract void execute( ClientApp client ) throws IllegalArgumentException


    /*
     * Number of replies other than the default message request ack
     */
    def int expectedReplies() { 1 }

//    /*
//     * Template method get invoked when the command has completed and
//     * the remote
//     */
//    def void onComplete( R reply ) { }
//

}


@TupleConstructor
class AppOptionsException extends Exception {

    CommandParser parsedCommand

    String message


    def String getMessage() {

        def result = new StringBuilder()
        if ( message ) {
            result << message << '\n'
        }

        def failure = parsedCommand?.getFailureMessage()
        if ( failure ) {
            result << failure
        }
        else if ( !message ) {
            result = '(unknown error)'
        }

        result.toString()
    }

}

@TupleConstructor
class AppHelpException extends Exception {

    CommandParser parsedCommand


    def String getMessage() {
        parsedCommand?.getHelp() ?: "Command syntax error"
    }

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

    @Parameter(names='--host', description='The remote cluster to which connect')
    String remoteHost

    @Parameter(names='--local', description = 'Run in local-only mode')
    boolean local

    @Parameter(names='--cpu', description = 'Number of cpu to use (only when launched in --local mode)')
    int cpu = 1

    @Parameter(names='--port', description = 'TCP port used by the client, if not specified will be chosen a free port automatically')
    int port

    @Parameter(names=['-v'], description = 'Prints the version number')
    boolean version

    @Parameter(names=['--version'], description = 'Prints the version number with build information')
    boolean fullVersion

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
        cmdParser.addCommand( new CmdClear() )
        //cmdParser.addCommand( new CmdGet() )
        cmdParser.addCommand( new CmdContext() )
        cmdParser.addCommand( new CmdHistory() )
        cmdParser.addCommand( new CmdList() )
        cmdParser.addCommand( new CmdKill() )

        return cmdParser
    }


}
