/*
 * Copyright (c) 2012, the authors.
 *
 *    This file is part of RUSH.
 *
 *    Moke is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation, either version 3 of the License, or
 *    (at your option) any later version.
 *
 *    Moke is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with RUSH.  If not, see <http://www.gnu.org/licenses/>.
 */

package rush.utils

import akka.actor.Address
import spock.lang.Specification

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class RushHelperTest extends Specification {

    def 'test fmt akka address ' () {

        expect:
        RushHelper.fmt( (Address)null ) == RushHelper.EMPTY
        RushHelper.fmt( new Address('akka','def','11.22.33.44',2132)) == '11.22.33.44:2132'

        RushHelper.fmt( new Address('akka','def','1.1.1.1',2222), 4) == '1.1.1.1:2222'
        RushHelper.fmt( new Address('akka','def','1.1.1.1',2222), 15) == '1.1.1.1:2222   '

    }

    def 'test fmt number' () {
        expect:
        RushHelper.fmt( 1 ) == '1'
        RushHelper.fmt( 100 ) == '100'
        RushHelper.fmt( 1000 ) == "1'000"
        RushHelper.fmt( 9_876_000 ) == "9'876'000"

        RushHelper.fmt( 0, 3 ) == '  0'
        RushHelper.fmt( 123, 3 ) == '123'
        RushHelper.fmt( 1234, 3 ) == "1'234"
        RushHelper.fmt( 1234, 6 ) == " 1'234"
    }


}
