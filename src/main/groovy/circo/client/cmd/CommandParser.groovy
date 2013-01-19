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
import com.beust.jcommander.ParameterException
import groovy.transform.ToString
import groovy.util.logging.Slf4j
import circo.utils.CmdLineHelper


/**
 * Wrap an command instance to run and the {@code JCommander} object used to parsed it
 */
@Slf4j
@ToString(includes = 'cmdArgs,cmdObj,help,enteredCommand,guessedCommand,failure', includePackage = false)
class CommandParser {

    private JCommander jparser

    private AbstractCommand cmdObj

    private boolean help

    private ParameterException failure

    private String enteredCommand

    private String guessedCommand

    private String[] cmdArgs

    boolean hasFailure() { failure != null }

    boolean isHelp() { return help  }

    def <T extends AbstractCommand > T getCommand() { return cmdObj as T }

    boolean hasCommand() { cmdObj != null }

    /**
     * @return The command help string
     */
    def String getHelp() {
        def result = new StringBuilder();
        if(jparser) jparser.usage(result);
        result.toString()
    }

    /**
     * Print the help message to the current standard output
     */
    def void printHelp() {
        println getHelp()
    }

    /**
     * Parse the command line arguments using the specified {@code JCommander} instance
     *
     * @param jcommander
     * @param args
     * @return
     */
    def CommandParser ( JCommander jcommander, String[] args  ) {

        assert jcommander

        this.cmdArgs = args
        this.jparser = jcommander

        try {
            // parse the main command
            try {
                jcommander.parse(args)
            }
            catch( ParameterException e ) {

                failure = e
            }

            // set the help flag
            if ( jparser && jparser.objects?.size() && jparser.objects[0].hasProperty('help') ) {
                help = jparser.objects[0].help
            }

            // when no sub command has been parsed just return
            def parsed = jcommander.getParsedCommand()
            if( !parsed || !jcommander.getCommands().containsKey(parsed)) {
                // done
                return
            }

            // -- set the command entered by the user
            enteredCommand = parsed

            // access to the sub command object
            JCommander subParser = jcommander.getCommands().get(parsed)
            if ( !subParser.objects || !(subParser.objects[0] instanceof AbstractCommand) ) {
                throw new IllegalArgumentException("Command '${parsed}' must implements ${AbstractCommand.simpleName} interface")
            }

            this.jparser = subParser
            this.cmdObj = subParser.objects[0] as AbstractCommand

            // override the help flag for this command
            if ( cmdObj.hasProperty('help') ) {
                help = cmdObj.help
            }

        }

        catch( ParameterException exception ) {
            // track the reported exception
            failure = exception
        }
        finally {

            // first token that does not start with a dash '-'  i.e. an option separator
            if( jparser && !enteredCommand ) {
                guessedCommand = args?.find { String str -> !str.startsWith('-') }
            }

        }

    }

    /**
     * @return The failure message string
     */
    String getFailureMessage() {

        def result = new StringBuilder()
        if ( !failure ) { return null }

        result << failure.getMessage()  ?: "Wrong command syntax: ${cmdArgs.join(' ')}"

        if ( enteredCommand && jparser ) {
            result << '\n\n'
            jparser.usage(result)
        }

        else if ( guessedCommand && jparser?.getCommands()?.size() ){

            def similar = CmdLineHelper.findBestMatchesFor( guessedCommand, jparser.getCommands().keySet() )
            if( similar ) {
                result << "\n\nDid you mean this?"
                similar.each { result << "   ${it}\n" }
            }

        }


        return result.toString()
    }

    /**
     * Print the failure message to the standard output
     */
    def void printFailureMessage() {

        println getFailureMessage()

    }



    def static CommandParser parse( String[] args ) {

        def appCommands = AppCommandsFactory.create()
        new CommandParser(appCommands,args)

    }


    def static CommandParser parse( String commandLine ) {

        def args = CmdLineHelper.splitter(commandLine)
        def appCommands = AppCommandsFactory.create()
        new CommandParser(appCommands,args as String[])

    }


}