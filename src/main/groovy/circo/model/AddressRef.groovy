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

package circo.model
import akka.actor.Address as AkkaAddress
import circo.Const
import circo.util.SerializeId
import groovy.transform.EqualsAndHashCode
/**
 * Model a host IP address
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */

@SerializeId
@EqualsAndHashCode
class AddressRef implements Serializable {

    final String host

    final Integer port

    AddressRef( String host, Integer port ) {
        assert host
        this.host = host
        this.port = port
    }

    AddressRef( String address ) {
        assert address

        int p = address.indexOf(':')
        if( p == -1 ) {
            this.host = address?.trim()
        }
        else {
            this.host = address.substring(0,p)?.trim()
            this.port = address.substring(p+1)?.trim()?.toInteger()
        }

    }

    AddressRef( AkkaAddress address ) {
        assert address

        this.host = address.host().get()
        this.port = address.port().get()?.toString()?.toInteger()

    }

    /**
     * Parse a command separated list of IP address e.g. 192.168.0.1, 192.168.0.1:4455, 192.168.0.4
     * @param addresses
     * @return
     */
    def static List<AddressRef> list( String addresses ) {

        List<AddressRef> result = []

        addresses.eachLine { String line ->
            line.split('[,\b]').each { String it -> result << new AddressRef(it) }
        }

        result
    }


    String toString() {

        String result = host
        if ( port != null ) {
            result += ':' + port.toString()
        }

        return result
    }

    AkkaAddress toAkkaAddress( String protocol = Const.DEFAULT_AKKA_PROTOCOL, String system = Const.DEFAULT_CLUSTER_NAME ) {
        new AkkaAddress( protocol, system, host, port ?: Const.DEFAULT_AKKA_PORT )
    }


    String fmtString( Closure formatter ) {
        formatter.call( host, port )
    }



    def static AddressRef fromString( String str ) {

        Integer port = null
        String protocol = null
        String system = null
        String host

        int p = str.indexOf('@')

        if ( p != -1 ) {
            def meta = str.substring(0,p)
            str = str.substring(p+1)


            p = meta.indexOf('://')
            if( p != -1 ) {
                protocol = meta.substring(0,p)
                system = meta.substring(p+3)
            }
            else {
                system = meta
            }
        }

        p = str.indexOf(':')
        if ( p != -1 ) {
            host = str.substring(0,p)
            port = str.substring(p+1).toInteger()
        }
        else {
            host = str
        }


        return new AddressRef(host, port)

    }


}
