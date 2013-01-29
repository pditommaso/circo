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



import spock.lang.Specification

import java.lang.reflect.Modifier

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class MetaTest extends Specification {


    def testMethodMissing () {
        expect:
        new Pretender().hello('world') == "method: hello with [world]"
    }


    def testPropMissing() {
        when:
        def clazz = new Pretender()
        clazz.xx = 1

        then:
        clazz.hello == "prop: hello"

    }

    def testMetaCompare() {

        when:
        def clazz1 = new MetaCompare(alpha: 'hola')
        def clazz2 = new MetaCompare(alpha: 'ciao')
        clazz2.compareWith(clazz1)

        then:
        clazz1.alpha == "hola"
        clazz2.alpha == 'ciao**'

    }

    def testMetaCompareEx() {

        when:
        def clazz1 = new MetaCompareEx(alpha: 'hola', beta: 'hello', delta: 'helo')
        def clazz2 = new MetaCompareEx(alpha: 'ciao', beta: 'hello', delta: 'halo')
        clazz2.compareWith(clazz1)

        then:
        clazz1.alpha == "hola"
        clazz2.alpha == 'ciao**'
        clazz1.beta == clazz2.beta
        clazz2.delta == 'halo**'
        clazz2.sayThisAndThat() == "${clazz2.alpha} And ${clazz2.beta}"

    }

    def testMixin () {

        when:
        def clazz1 = new MetaClassMixin(uno: '1', due: '2')
        def clazz2 = new MetaClassMixin(uno: '1', due: 'due')
        clazz2.compareWith(clazz1)

        then:
        clazz1.uno == clazz2.uno
        clazz2.due == 'due**'
    }

}


class Pretender {

    def methodMissing( String name, def args ) {
        return "method: $name with $args"
    }

    Object propertyMissing(String name) {
       println ">> $name"
        return "prop: ${name}"
    }
}

class MetaCompare {

    def alpha

    def beta

    private def compare


    def String sayThisAndThat()  {  "$alpha And $beta"  }

    def getProperty(String name) {

        def thisValue = metaClass.getProperty(this,name)
        if ( metaClass.hasProperty(this,name) && Modifier.isPublic(metaClass.getMetaProperty(name).getModifiers()) ) {
            def MetaCompare otherClazz = this.compare
            def thatValue = otherClazz ? metaClass.getProperty(otherClazz,name) : null
            if ( otherClazz && thisValue != thatValue ) {
                return thisValue?.toString() + "**"
            }
        }

        return thisValue
    }


    public void compareWith( def obj ) {
        this.compare = obj
    }

}


class MetaCompareEx extends MetaCompare {
    def delta
}

@Mixin(MetaCompare)
class MetaClassMixin {

    def uno
    def due

}


