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

import circo.model.StringRef
import circo.model.Context
import com.beust.jcommander.DynamicParameter
import com.beust.jcommander.Parameter
import com.beust.jcommander.Parameters
import groovy.util.logging.Slf4j

import static circo.Const.LIST_CLOSE_BRACKET
import static circo.Const.LIST_OPEN_BRACKET
/**
 * Command to manage the job context
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@Parameters(commandNames = ['ctx'], commandDescription = 'Manage the job execution context')
class CmdContext extends AbstractCommand {

    @Parameter(names='--get', description = 'Print the value of specified variable')
    String fGet

    @DynamicParameter(names=['-s','--set'], description = 'Set a variable in the current job e.g. COUNT=n')
    Map<String,String> fSet = new HashMap<>()

    @Parameter(names=['-u','--unset'], description = 'Remove the specified value from the current context')
    List<String> fUnset

    @DynamicParameter(names=['-a','--add'], description = 'Add the specified value to an existing variable or create it if it does not exist yet')
    Map<String,String> fAppend = new HashMap<>()

    @Parameter(names=['-d','--delim'], description = 'Delimiter used when getting values of type list')
    String fDelim = ','

    @Parameter(names='--empty', description='Remove all entries in the current environment')
    Boolean fEmpty

    @Parameter(names='--import', description='Import the environment from the file specified. Use the special keyword \'env\' to import the system environment')
    String fImport

    @Parameter(names=['-g','--grep'], description = 'Filter the result by the specified \'grep\' like filter')
    String grep


    @Override
    void execute(ClientApp client) throws IllegalArgumentException {

        def text = apply(client.context)
        if ( text && grep ) {
            text.toString().eachLine { String it ->
                if( it =~ /$grep/) println(it)
            }
        }
        else if ( text ){
            println text.trim()
        }


    }

    def apply(Context ctx, Map<String,String> environment = System.getenv()) {
        /*
         * The 'get' a value from the context and print it out to the console
         */
        if( fGet ) {
            return valuesToStr(ctx.getValues(fGet))
        }

        /*
         * 'set' a value in the context
         */
        if( fSet ) {
            fSet.each { k, v ->
                ctx.put(k,v)
            }
            return
        }

        /*
         * 'remove' a value from the context
         */
        if ( fUnset ) {
            fUnset.each { ctx.removeAll(it) }
            return
        }

        /*
         * 'add' a value to an entry already existing in the context
         */
        if( fAppend ) {
            fAppend.each { k, v -> ctx.add(k,v)}
            return
        }

        /*
         * 'empty' remove all the entries in context
         */
        if ( fEmpty ) {
            ctx.clear()
            return
        }

        /*
         * 'import' the context from a file -or- the current environment using the keyword 'env'
         */
        if ( fImport ) {
            log.debug "grep: $grep"
            log.debug "env : $environment"

            if( fImport == 'env' ) {
                environment?.each { k, v ->
                    if( !grep || k =~ /$grep/) {   // -- filter by the 'grep' string when provided
                    ctx.put(new StringRef(k,v))    // -- import ad 'string' token, i.e. do not parse 'list' and 'range' types
                    }
                }
            }
            else {
                def file = new File(fImport)
                if ( !file.exists() ) {
                    return "The specified file does not exist: $file"
                }

                def props = new Properties()
                try {
                    props.load(new FileInputStream(file))
                    props.each { String k, String v ->
                        if ( !grep || k =~ /$grep/) {
                            ctx.put(k, v)
                        }
                    }
                }
                catch( Exception e ) {
                    log.error "Unable to import the file specified -- Make sure entries are in the format 'name=value'", e
                }
            }

        }

        // -- when nothing else is specified, print the current context
        return getContextString(ctx)
    }

    /*
     * print out the current context to the console screen
     */
    def String getContextString(Context context) {

        def result = new StringBuilder()
        def names = context.getNames().sort()
        names.each { String it ->
            result << "$it=${valuesToStr(context.getValues(it))}\n"
        }

        result.toString()
    }


    /*
     * Convert a generic item to a string
     */
    String str( def item ) {
        if( item instanceof File ) {
            return item.name
        }
        else {
            return item?.toString()
        }
    }

    /**
     *  Converts a collection of items to it string representation
     */
    String valuesToStr( def items ) {

        if( items instanceof Range ) {
            items.toString()
        }

        else if ( items instanceof Collection ) {
            if ( items.size() == 0 ) return '-'

            if ( items.size() == 1 ) return str(items[0])

            // verify if the list is made up all of synonyms
            def list = items.collect{ str(it) }.unique(false)
            if( list.size() == 1 ) {
                return "${LIST_OPEN_BRACKET}${str(list[0])},..${items.size()-1} more${LIST_CLOSE_BRACKET}"
            }
            else if ( list.size() <= 10 ){
                return LIST_OPEN_BRACKET + items.collect { str(it) }.join(fDelim) + LIST_CLOSE_BRACKET
            }
            else {
                return LIST_OPEN_BRACKET + items[0..9].collect { str(it) }.join(fDelim) + ",..${items.size()-10} more" + LIST_CLOSE_BRACKET
            }
        }

        else {
            str(items)
        }

    }



}
