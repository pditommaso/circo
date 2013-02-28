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

import groovy.sql.Sql
import spock.lang.Shared
import spock.lang.Specification

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class HzJdbcFilesMapStoreTest extends Specification {

    @Shared Sql sql

    def 'test set and put ' () {

        setup:
        sql = Sql.newInstance('jdbc:h2:mem:Circo')
        def files = new HzJdbcFilesMapStore(sql)
        files.dropTable()
        files.createTable()

        def content = """
        Lorem ipsum dolor sit amet, consectetur adipiscing elit. Suspendisse eu velit felis. Nullam fringilla interdum ipsum,
        at accumsan mauris cursus non. Sed et felis et nisl viverra dignissim vel ut nulla. Sed ultricies, turpis et
        sollicitudin faucibus, nibh eros dignissim lacus, non fringilla dui erat quis nisi.

        Sed turpis mi, elementum ut sollicitudin iaculis, mattis non nunc. Phasellus at leo eu tellus auctor convallis.
        Aenean ipsum diam, feugiat vitae ullamcorper mattis, porttitor eu neque. Suspendisse faucibus, massa ut tincidunt
        vestibulum, felis ligula iaculis sapien, quis imperdiet diam tellus sed lacus. Nulla aliquet ullamcorper quam,
        vitae consequat sem mollis non.

        Nunc mattis turpis nec eros lobortis at condimentum diam consequat. Nullam fermentum scelerisque sodales. Curabitur ac
        magna odio, nec sagittis lectus. Praesent at leo eget libero vestibulum elementum id non elit.
        """
                .stripIndent()
        def sourceFile = File.createTempFile('test',null)
        sourceFile.deleteOnExit()
        sourceFile.text = content

        when:
        def fileId = UUID.randomUUID()
        files.store(fileId, sourceFile)

        def loaded = files.load( fileId )

        then:
        loaded != null
        loaded.name == sourceFile.name
        loaded.text == sourceFile.text
        loaded != sourceFile

        files.load( UUID.randomUUID() ) == null



    }

}
