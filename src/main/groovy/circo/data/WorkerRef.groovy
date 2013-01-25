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

package circo.data
import akka.actor.*
import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString
import groovy.util.logging.Slf4j
import circo.util.SerializeId
import scala.Option

/**
 *
 *  @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@SerializeId
@ToString(includes = 'path', includePackage = false)
@EqualsAndHashCode(includes=['path'])
class WorkerRef implements Serializable {

    private transient static ActorSystem system

    private transient static Address transportAddress

    static def void init( ActorSystem system, Address address = null ) {
        WorkerRef.system = system
        WorkerRef.transportAddress = address
    }

    String path

    @Lazy
    transient ActorRef actor = { fActor ?: ( system.actorFor(path) ) } ()

    protected transient ActorRef fActor


    def WorkerRef(ActorRef actor) {
        assert actor
        this.fActor = actor
        this.path = transportAddress ? actor.path().toStringWithAddress(transportAddress) : actor.path().toString()
    }

    def WorkerRef(ActorPath path) {
        assert path
        this.path = transportAddress ? path.toStringWithAddress(transportAddress) : path.toString()
    }


    private WorkerRef(String path) {
        this.path = path
    }

    def static WorkerRef copy( WorkerRef that ) {
        assert that
        new WorkerRef(that.path)
    }

    def tell( def message ) {
        actor.tell(message)
    }

    def tell( def message, ActorRef sender ) {
        actor.tell(message, sender)
    }

    def tell( def message, WorkerRef sender ) {
        actor.tell(message, sender.actor)
    }

    def forward( def message, ActorContext context) {
        actor.forward(message, context)
    }

    def path() {
        return actor.path()
    }

    def Address address() {
        AddressFromURIString.parse(path)
    }

    def boolean isLocal( Address address ) {
        log.trace "Worker is local [${this.address()} == ${address}]? ${this.address() == address ? 'YES' : 'NO'} "
        this.address() == address
    }

    def String toFmtString() {

        def host = opt( address().host() )
        def port = opt( address().port() )
        def name = path ? path.split('/')[-1] : '-'

        "$host:$port/$name"

    }

    private opt( Option value ) {
        try { value.get() }
        catch( NoSuchElementException e ) {
            null
        }
    }

}
