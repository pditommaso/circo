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
 *  Tail recursion with Groovy (!)
 *
 *  http://victorsavkin.com/post/5231079655/cool-stuff-in-groovy-1-8-trampoline
 *
 */

def fib = null
fib = { n, a = BigInteger.ZERO, b = BigInteger.ONE ->

    if ( n == 0 ) a
    else fib.trampoline n-1, b, a+b
}.trampoline()

println fib(1001)


def factorial = null
factorial = { Integer n, BigInteger acc = 1 ->
    if (n == 1) return acc
    factorial.trampoline(n - 1, n * acc)
}.trampoline()

println factorial(1000)  // It works