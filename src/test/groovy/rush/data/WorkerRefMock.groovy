/*
 * Copyright (c) 2012, the authors.
 *
 *    This file is part of Rush.
 *
 *    Rush is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation, either version 3 of the License, or
 *    (at your option) any later version.
 *
 *    Rush is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with Rush.  If not, see <http://www.gnu.org/licenses/>.
 */

package rush.data

import akka.actor.Address
import akka.actor.RootActorPath

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class WorkerRefMock extends WorkerRef {

    WorkerRefMock(String path) {
        super( new RootActorPath(parse(path)[0] as Address, parse(path)[1] as String) )
    }

    def boolean equals( def ref ) { super.equals(ref) }

    def int hashCode() { super.hashCode() }

    static parse( String path ) {
        def result = []
        def p = path.indexOf('://')
        if ( p != -1 ) {
            def prot = path.substring(0,p); path = path.substring(p+3)
            def splits = path.split('/')
            def system = splits[0];

            def address
            p = system.indexOf('@')
            if ( p != -1 ) {
                def hostWithPort = system.substring(p+1)
                system = system.substring(0,p)

                def host
                def port
                p = hostWithPort.indexOf(':')
                if ( p != -1 ) {
                    host = hostWithPort.substring(0,p)
                    port = Integer.parseInt(hostWithPort.substring(p+1))
                }
                else {
                    host = hostWithPort
                    port = 2551
                }
                address = new Address(prot,system,host,port)
            }
            else {
                address = new Address(prot,system)
            }

            def remainPath = '/' + splits[1..-1].join('/')

            result << address
            result << remainPath
        }
        else {
            result << new Address( 'akka', 'default')
            result << path
        }
        return result
    }

}