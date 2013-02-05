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



/**
 *
 *  @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */


class MyClass {

    private hello = "Hello world"

    def getHello() { hello }

    def sayHello() { getHello() }

    def printHello() { print "$hello - ${getHello()}" }

}

class MyExtension extends MyClass {

}

def instance = new MyClass()

assert instance.hello == 'Hello world'
assert instance.getHello() == 'Hello world'
assert instance.sayHello() == 'Hello world'

MyClass.metaClass.getHello = { "Hola mundo" }
def instance2 = new MyClass()
assert instance2.hello == 'Hola mundo'
assert instance2.getHello() == 'Hola mundo'
assert instance2.sayHello() == 'Hola mundo'


def instance3 = new MyExtension()
assert instance3.hello == 'Hola mundo'
assert instance3.getHello() == 'Hola mundo'
assert instance3.sayHello() == 'Hola mundo'

def instance4 = new MyClass()
instance4.metaClass.getHello = { "Ciao mondo" }
assert instance4.hello == 'Ciao mondo'
assert instance4.getHello() == 'Ciao mondo'
assert instance4.sayHello() == 'Ciao mondo'

MyClass.metaClass.getHello = { "Xxxx" }
def instance5 = new MyExtension()
assert instance5.hello == 'Xxxx'
assert instance5.getHello() == 'Xxxx'
assert instance5.sayHello() == 'Xxxx'


//assert simple.doThis() == "do this"
//assert simple.doThat() == "do that"
//assert simple.sayHello('world') == "Hello world"
//assert simple.sayHello('Ciao','zio') == "Ciao zio"
//assert simple.self == "myself"
//assert simple.toString() == "myself"
//
//assert simple.self == 'myself'
//assert simple.getSelf() == 'myself'
//
//simple.metaClass.doThis = { "fai questo" }
//assert simple.doThis() == "fai questo"
//
//simple.metaClass.sayHello = { String param -> "Ciao $param" }
//simple.metaClass.sayHello = { String p1, String p2 -> "$p2 $p1" }
//
//assert simple.sayHello('world') == "Ciao world"
//assert simple.sayHello('due','uno') == "uno due"
//
//simple.metaClass.getSelf = { 'Yo' }
//assert simple.self == 'Yo'
//assert simple.getSelf() == 'Yo'
//
//MySimpleClass.metaClass.getSelf = { "XXX" }
//assert new MySimpleClass().self == "XXX"
////assert new MySimpleClass().toString() == "XXX"
//
//MySimpleClass.metaClass.getSelf = { "YYY" }
//assert new MySimpleClass().self == "YYY"
//
//simple.metaClass.get = { String name -> name == 'self' ? '***' : null }
//assert simple.self == "***"
//assert simple.toString() == "***"
//assert simple.alpha == "a"
//assert simple.beta == "b"





//def simple2 = new MySimpleClass()
//simple2.metaClass.get = { String name -> name == 'self' ? '***' : null }
//
//assert simple2.self == "123"
