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

package circo.data

import akka.actor.AddressFromURIString
import spock.lang.Specification

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class StringRefTest extends Specification {

    def 'test StringRef' () {

        when:
        def ref1 = new StringRef('label', 'Hello world')
        def ref2 = new StringRef('label2', 'Ciao mondo', AddressFromURIString.parse('akka://sys@1.1.1.1:2551') )

        then:
        ref1.name == 'label'
        ref1.value == 'Hello world'
        ref1.address == null

        ref2.name == 'label2'
        ref2.value == 'Ciao mondo'
        ref2.address.host().get() == '1.1.1.1'
        ref2.address.port().get() == 2551

    }
}

class FileRefTest extends Specification {

    def 'test FileRef' () {

        when:
        def ref1 = new FileRef(new File('/some/path/file-name.txt'))
        def ref2 = new FileRef(new File('relative/path/file-name2.txt'), AddressFromURIString.parse('akka://sys@1.1.2.2:2554') )

        then:
        ref1.name == 'file-name.txt'
        ref1.file == new File('/some/path/file-name.txt')
        ref1.address == null

        ref2.name == 'file-name2.txt'
        ref2.file == new File('relative/path/file-name2.txt')
        ref2.address.host().get() == '1.1.2.2'
        ref2.address.port().get() == 2554

    }
}
