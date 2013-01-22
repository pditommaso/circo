/*
 * Copyright (c) 2012, the authors.
 *
 *    This file is part of name.
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

package circo

import akka.actor.Address
import circo.util.CmdLine
import spock.lang.Shared
import spock.lang.Specification

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class ClusterDaemonTest extends Specification{

    @Shared
    def cli = new CmdLine()

    @Shared
    def port = cli.port
    
    @Shared
    def name = new ClusterDaemon(cli).CLUSTER_NAME

    
    
    def 'test parseAddresses' () {

        setup:
        def cli = new CmdLine()

        expect:
        new ClusterDaemon(cli).parseAddresses(addreass) == list

        where:
        addreass            | list
        null                | []
        ''                  | []
        '1.1.1.1'           | [new Address('akka',name,'1.1.1.1',port)]
        '2.2.2.2:4455'      | [new Address('akka',name,'2.2.2.2',4455)]
        '1.1.1.1:11,3.3.3.3:33\n4.4.4.4:44\n5.5.5.5,6.6.6.6'  | [new Address('akka',name,'1.1.1.1',11),new Address('akka',name,'3.3.3.3',33),new Address('akka',name,'4.4.4.4',44),new Address('akka',name,'5.5.5.5',port),new Address('akka',name,'6.6.6.6',port)]

    }
}
