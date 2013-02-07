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

def list = [1,2,3,4,5]

assert 15 == list.sum()
assert 15 == list.inject {e, a -> a += e }
assert 17 == list.inject(2) {e, a -> a += e }
assert 1 == list.min()
assert 5 == list.max()


assert [1,3,5] == list.findAll { it % 2 }
assert [1,3,5] == list.grep { it % 2 }


assert list.every{ it < 6 }
assert list.any { it < 3  }

assert [1,2] == list.take(2)
assert [4,5] == list.drop(3)
assert [4,5] == list.dropWhile { it<=3 }

assert 1 == list.head()
assert [2,3,4,5] == list.tail()

assert [[1,2], [3,4], [5]] == list.collate(2)

assert [6,8] == list[2..3] .collect { it * 2 }
[[1,2], [3,4], [5]].flatten() == list