
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
 *  @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */

public void generate(List<List> sets) {
    int solutions = 1;
    for(int i = 0; i < sets.size(); solutions *= sets[i++].size());
    println "solution: " + solutions

    for(int i = 0; i < solutions; i++) {
        int j = 1;
        for(List set : sets) {
            def p = (i/j) as Integer
            System.out.print(set[p % set.size()] + " ");
            j *= set.size();
        }
        System.out.println();
    }
}

def matrix = [[1,2,3], [3,2], [5,6,7], ['a','b','c','d','z']]
generate(matrix)