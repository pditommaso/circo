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




import org.codehaus.groovy.runtime.InvokerHelper
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class ExpandoTest  {

    static class GetterDummy {

        def hello = "x"

        def String toString()  { "${hello}" }

        def void sayHello() {
            println hello
        }

    }

    static class DummyDelegatingMetaClass extends DelegatingMetaClass {

        DummyDelegatingMetaClass(Class clazz) {
            super(clazz)
            initialize()
        }

        public Object invokeMethod(Object obj, String methodName, Object[] args)
        {
            println "*** invoking method ${methodName}"
            super.invokeMethod(obj, methodName, args)
        }

        public Object getProperty(String name)  {
            println "*** getter: ${name}"
            return null
        }
    }

    def "test getter"()  {

        setup:
        def myMetaClass = new DummyDelegatingMetaClass(GetterDummy)
        InvokerHelper.metaRegistry.setMetaClass(GetterDummy.class, myMetaClass)

        when:
        def clazz = new GetterDummy()

        println clazz.toString()
        println clazz.hello
        println clazz.getHello()
        println clazz.getHello()

        then:
        true

    }


    static class DummyExpando  {

        def hello = "hello"

        def values = [:]

        def void sayHello () {
            println hello
        }

        def String toString()  { "${hello}" }

        Object get( String name ) {
            println "*** getter: $name"
            values[name]
        }

        def void set( String name, def val ) {
            values[name] = val
        }

    }


    def "test expando" () {

        when:
        def ex = new DummyExpando()
        ex.field1 = 'uno'
        ex.sayHello()

        then:
        ex.hello == "hello"
        ex.field1 == 'uno'

    }

}
