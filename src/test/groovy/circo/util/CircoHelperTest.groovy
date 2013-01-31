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

package circo.util

import akka.actor.Address
import circo.Const
import spock.lang.Specification

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class CircoHelperTest extends Specification {

    def 'test fmt akka address ' () {

        expect:
        CircoHelper.fmt( (Address)null ) == CircoHelper.EMPTY
        CircoHelper.fmt( new Address('akka','def','11.22.33.44',2132)) == '11.22.33.44:2132'

        CircoHelper.fmt( new Address('akka','def','1.1.1.1',2222), 4) == '1.1.1.1:2222'
        CircoHelper.fmt( new Address('akka','def','1.1.1.1',2222), 15) == '1.1.1.1:2222   '

    }

    def 'test fmt number' () {
        expect:
        CircoHelper.fmt( 1 ) == '1'
        CircoHelper.fmt( 100 ) == '100'
        CircoHelper.fmt( 1000 ) == "1'000"
        CircoHelper.fmt( 9_876_000 ) == "9'876'000"

        CircoHelper.fmt( 0, 3 ) == '  0'
        CircoHelper.fmt( 123, 3 ) == '123'
        CircoHelper.fmt( 1234, 3 ) == "1'234"
        CircoHelper.fmt( 1234, 6 ) == " 1'234"
    }


    def 'test address fromAddress' () {

        expect:
        address as Address == CircoHelper.parseAddress(str)

        where:
        str                             |  address
        '1.1.1.1'                       | new Address( 'akka', Const.DEFAULT_AKKA_SYSTEM, '1.1.1.1', Const.DEFAULT_AKKA_PORT )
        '2.2.2.2:33'                    | new Address( 'akka', Const.DEFAULT_AKKA_SYSTEM, '2.2.2.2', 33 )
        'sys@3.3.3.3'                   | new Address( 'akka', 'sys', '3.3.3.3', Const.DEFAULT_AKKA_PORT )
        'http://xyz@192.168.0.1:5700'   | new Address( 'http', 'xyz', '192.168.0.1', 5700 )

    }


}
