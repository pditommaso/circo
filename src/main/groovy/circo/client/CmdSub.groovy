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

import circo.model.FileRef
import circo.model.Context
import com.beust.jcommander.DynamicParameter
import com.beust.jcommander.Parameter
import com.beust.jcommander.Parameters
import groovy.io.FileType
import groovy.transform.ToString
import groovy.util.logging.Slf4j
import scala.concurrent.duration.Duration
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */

@Slf4j
@ToString(includePackage = false)
@Parameters(commandNames='sub', commandDescription='Submit a new job for cluster execution')
class CmdSub extends AbstractCommand  {

    @Parameter
    List<String> command

    @Parameter(names = ['-sync','--sync'], description = "Causes the 'sub' command to wait for  the  job  to  complete before  exiting")
    boolean syncOutput

    @DynamicParameter(names = "-v", description = "Expands the list of environment variables that are exported to the job")
    Map<String, String> env = new HashMap<String, String>();

    @Parameter(names = '-V', description = 'Declares that all environment variables in the qsub commands environment are to be exported to the batch job')
    boolean exportAllEnvironment

    @Parameter(names=['--max-duration'], description = 'Max duration allows for the job (sec|min|hour|day)', converter = DurationConverter)
    Duration maxDuration

    @Parameter(names=['--max-attempts'], description = 'Max number of times the job can be resubmitted in case of error')
    int maxAttempts = 2

    @Parameter(names=['--max-inactive'], description='Max allowed period of job inactivity (sec|min|hour|day)', converter = DurationConverter)
    Duration maxInactive = Duration.create('10 min')

    @Parameter(names='--print', description = 'Wait for the job completion and print out the result to the stdout')
    boolean printOutput


    @Parameter(names='--each', description = 'Submit the command for each entry in the specified collection', listConverter = EachListConverter)
    List<String> eachItems

    @Parameter(names='--each-file', description = 'Submit the command for each file specified. Linus wildcards are allowed')
    List<String> eachFile


    /**
     * The command will be submitted the number of times specified the range
     *
     * note: due to a bug of JCommand this field has to be declared as Object
     * see https://groups.google.com/d/topic/jcommander/_p4Jl_c4PO0/discussion
     *
     */
    @Parameter(names=['-t','--times'], arity = 1, description = 'Number of times this request has to be submitted', converter = IntRangeConverter)
    private times

    def CustomIntRange getTimes() {  times as CustomIntRange }

    @Parameter(names=['--produce'], description = 'The files this job produces as output to the context')
    List<String> produce

    //--- non command option parameters


    /** The user who submitted the request */
    private String user

    def String getUser() { user }

    /** A copy of the current context to be used for the job submission */
    private Context context

    def Context getContext() { context }


    private init(Context clientContext) {

        // -- the user who is submitting the request
        this.user = System.properties['user.name']

        // -- the environment variable (+++ to be merged with the context structure ????)
        if( this.exportAllEnvironment ) {
            def copy = new HashMap<>(env)
            env = new HashMap<>( System.getenv() )
            if( copy ) {
                env.putAll(copy)
            }
        }

        // -- the current execution context as defined in the client
        this.context = Context.copy(clientContext)

        def items = []
        this.eachItems?.each { String it ->
            // when an entry contains an assignment
            // split it and add the value into the context
            def p = it.indexOf('=')
            if ( p != -1 ) {
                def name = it.substring(0,p)
                def value = it.substring(p+1)
                this.context.put(name,value)
                items << name
            }
            else {
                items << it
            }
        }
        this.eachItems = items


        /*
         * each file
         */
        def addedFileRefNames = new HashSet<String>()
        this.eachFile?.each { String it ->

            String name = null
            String filePattern
            def p = it.indexOf('=')
            if( p == -1 ) {
                filePattern = it
            }
            else {
                name = it.substring(0,p)
                filePattern = it.substring(p+1)
            }

            def file = new File(filePattern)
            String parent = file.parent ?: '.'
            if ( parent == '~' ) {
                parent = System.properties['user.home']
            }
            else if ( parent.startsWith('~/') ) {
                parent = System.properties['user.home'] + parent.substring(1)
            }


            // replace any wildcards characters
            // TODO give a try to http://code.google.com/p/wildcard/  -or- http://commons.apache.org/io/
            filePattern = file.name.replace("?", ".?").replace("*", ".*?")

            // scan to find the file with that name
            new File(parent).eachFileMatch(FileType.FILES, ~/$filePattern/ ) { File found ->
                def ref = new FileRef(name?:found.name, found)
                context.add( ref )
                // append this names to the eachItems
                addedFileRefNames << ref.name
            }

        }

        log.debug "Added file refs: ${addedFileRefNames}"
        this.eachItems.addAll( addedFileRefNames )

    }


    @Override
    void execute(ClientApp client) {

        if( times && eachItems ) {
            throw new IllegalArgumentException("Options '--times' and '--each' cannot be used together")
        }

        init(client.context)

        if( !command ) {
            println "no command to submit provided -- nothing to do"
            return
        }

        def result = client.send(this)
        result?.printMessages()

    }

    /**
     * @return When the sub is executed is sync mode or printing the stdout this command stop to wait for two reply:
     *      {@code SubReply} and {@code JobReply}, otherwise it will wait only for the {@code SubReply} confirmation
     *
     */
    @Override
    def int expectedReplies() {
        return ( syncOutput || printOutput ) ? 2 : 1
    }

}
