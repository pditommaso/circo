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

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class DecorateFields {

    def field1
    def field2

    private field3 = 'xxx'

    def get( String name ) {
        println '>> get '
        return 'ciao'
    }

    def invokeMethod(String name, def args) {
        println ">>> invokeMethod"
        return null
    }

    def getProperty(String name) {
        println '>> property'

        def value = metaClass.getProperty(this,name)
        return value ? "* $value *": null
    }

    String toString() { "${getField1()} ${getField2()}" }

    String something() { "$field3" }

}


def obj = new DecorateFields()
obj.field1 = 'Hello'
obj.field2 = 'world'

assert obj.field3 == 'xxx'
//assert obj.field1 == "* Hello *"
//assert obj.field2 == "* world *"
//assert obj.toString() == "* Hello * * world *"
