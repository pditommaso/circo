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

package functional

/**
 *
 *  @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */


def fib = null
fib = { p ->

  if ( p<2 ) BigInteger.ONE
  else fib(p-1) + fib(p-2)

}


def mfib = null
mfib = { p ->

    if ( p<2 ) BigInteger.ONE
    else mfib(p-1) + mfib(p-2)

}.memoize()

def time = { closure ->
    def start = System.currentTimeMillis()
    closure.call()
    def delta = System.currentTimeMillis() - start
    println "elapsed time: $delta "

}

time { println fib(30) }
time { println mfib(30) }