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
class DataHolderTest extends Specification {

    static class MyHolder extends DataHolder {

        String toString(  ) { "render ${f1} and ${f2}" }

    }


    def "test data holder " ()  {

        when:
        def source = new DataHolder()
        source.f1 = '1'
        source.f2 = '1'

        def holder = new DataHolder()
        holder.set("f1", '1')
        holder.f2 = '2'

        holder.compareWith(source) { it -> '*' + it }

        then:
        holder.get('f1') == '1'
        holder.f2 == '*2'

    }


    def "test data myholder " ()  {

        when:
        def source = new DataHolder()
        source.f1 = '1'
        source.f2 = '1'

        def holder = new MyHolder()
        holder.set("f1", '1')
        holder.f2 = '2'

        holder.compareWith(source) { it -> '*' + it }

        println holder.toString()

        then:
        holder.get('f1') == '1'
        holder.f2 == '*2'
        holder.toString() == "render 1 and *2"

    }

}
