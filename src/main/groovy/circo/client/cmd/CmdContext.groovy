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
import circo.messages.JobContext
import com.beust.jcommander.DynamicParameter
import com.beust.jcommander.Parameter
import com.beust.jcommander.Parameters
import groovy.util.logging.Slf4j

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

        if( fGet ) {
            println client.getContext().getValues(fGet).join(fDelim)
            return
        }

        if( fSet ) {
            fSet.each { k, v ->
                client.getContext().put(k,v)
            }
            return
        }

        if ( fUnset ) {
            fUnset.each { client.context.removeAll(it) }
            return
        }

        if( fAppend ) {
            fAppend.each { k, v -> client.getContext().add(k,v)}
            return
        }

        if ( fEmpty ) {
            client.getContext().clear()
            return
        }

        if ( fImport ) {
            if( fImport == 'env' ) {
                System.getenv().each { k, v -> client.context.put(k,v) }
            }
            else {
                def file = new File(fImport)
                if ( !file.exists() ) {
                    println "The specified file does not exist: $file"
                    return
                }

                def props = new Properties()
                try {
                    props.load(new FileInputStream(file))
                    props.each { String k, String v -> client.context.put(k,v) }
                }
                catch( Exception e ) {
                    log.error "Unable to import the file specified -- Make sure entries are in the format 'name=value'", e
                }
            }

        }

        // -- when nothing else is specified, print the current context
        printContext(client.getContext())
    }

    def void printContext(JobContext context) {

        def names = context.getNames().sort()
        names.each { String it ->
            println "$it=${valuesToStr(context.getValues(it))}"
        }
    }

    def println( def text ) {
        if ( text && grep )  {
            text.toString().eachLine { String it ->
                if( it =~ /$grep/) super.println(it)
            }
        }
        else {
            super.println(text)
        }
    }


    String itemToStr( def item ) {
        if( item instanceof File ) {
            return item.name
        }
        else {
            return item?.toString()
        }
    }

    String valuesToStr( def items ) {

        if( items instanceof Range ) {
            items.toString()
        }
        else if ( items instanceof Collection ) {
            if ( items.size() == 0 ) return '-'
            if ( items.size() == 1 ) return itemToStr(items[0])

            def list = items.unique()
            if( list.size() == 1 ) {
                return "${itemToStr(list[0])} (${items.size()})"
            }
            else {
                return list.collect { itemToStr(it) }.join(fDelim)
            }
        }
        else {
            itemToStr(items[0])
        }

    }



}
