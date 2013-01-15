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

package rush.ui

import spock.lang.Specification

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class TextLabelTest extends Specification {

    def "test pad" () {

        expect:
        TextLabel.of('Hello').left().pad(8).toString() == "Hello   "
        TextLabel.of('Hello').right().pad(8).toString() == "   Hello"
        TextLabel.of('1234').number().pad(8).toString() == "    1234"

    }


    def "test apply decorator " () {

        when:
        def label = TextLabel.of( 'The cat eat the food' )
        def deco  = { obj, val -> "* ${val} *".toString() } as LabelDecorator
        label << deco

        then:
        label.toString() == "The cat eat the food"
        label.switchOn().toString() == "* The cat eat the food *"

    }

    def "test add decorator" () {

        when:
        def label = TextLabel.of('The Fact cat') << AnsiStyle.style().bold()

        then:
        label.decorators.find { it instanceof AnsiStyle }
        label.decorators.size() == 1

    }

}
