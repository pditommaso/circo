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

package circo.model

import circo.data.LocalDataStore
import spock.lang.Specification
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class StringRefTest extends Specification {

    def 'test StringRef' () {

        when:
        def ref1 = new StringRef('label', 'Hello world')

        then:
        ref1.name == 'label'
        ref1.value == 'Hello world'

    }
}

class FileRefTest extends Specification {

    def 'test FileRef' () {

        setup:
        FileRef.currentNodeId = 1

        when:
        def ref1 = new FileRef(new File('/some/path/file-name.txt'), 1)

        then:
        ref1.name == 'file-name.txt'
        ref1.data == new File('/some/path/file-name.txt')

    }


    def 'test store a file and get from another node' (){

        setup:
        FileRef.currentNodeId = 1
        FileRef.dataStore = new LocalDataStore()

        File sourceFile = File.createTempFile('test',null); sourceFile.deleteOnExit()
        sourceFile.text = """
        aaaaa
        bbbbbbbbb
        cccccccccc
        :
        zzzzzzzzzzz
        """

        /*
         * a reference to a file on the node '1' is create
         */
        def ref = new FileRef(sourceFile, 1)


        /*
         * simulate access from another node '3'
         * the file is restore by the configured 'data-store' and re-created on the file system
         */
        when:
        FileRef.currentNodeId = 3
        def targetFile = ref.getData()

        then:
        targetFile.exists()
        targetFile.text == sourceFile.text
        targetFile.name == sourceFile.name
        targetFile != sourceFile

    }

}
