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

package circo.data.sql

import com.jolbox.bonecp.BoneCPDataSource
import com.typesafe.config.ConfigFactory
import spock.lang.Specification
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class JdbcDataSourceFactoryTest extends Specification {


    def cleanupSpec() {
        JdbcDataSourceFactory.instance = null
    }


    def 'test create' () {

        when:
        def ds = JdbcDataSourceFactory.create( 'jdbc:h2:mem:Circo' )

        then:
        noExceptionThrown()
        !ds.getConnection().isClosed()

        cleanup:
        (ds as BoneCPDataSource).close()
    }

    def 'test create with map ' () {

        when:
        def ds = JdbcDataSourceFactory.create( 'jdbc:h2:mem:Circo', [username:'paolo', password:'ciao'] )

        then:
        noExceptionThrown()
        ds instanceof BoneCPDataSource
        (ds as BoneCPDataSource).getUsername() == 'paolo'
        (ds as BoneCPDataSource).getPassword() == 'ciao'

        cleanup:
        (ds as BoneCPDataSource).close()

    }


    def 'test CreateWithConfig' () {

        setup:
        def str = """
        store {
            jdbc {
              url = "jdbc:h2:mem:xxx"
              username = paolo
              password = zzz
              acquireIncrement = 2
            }
        }
        """

        def conf = ConfigFactory.parseString( str )

        when:
        def ds = JdbcDataSourceFactory.create( conf.getConfig('store.jdbc') )

        then:
        noExceptionThrown()
        ds instanceof BoneCPDataSource
        (ds as BoneCPDataSource).getUsername() == 'paolo'
        (ds as BoneCPDataSource).getPassword() == 'zzz'
        (ds as BoneCPDataSource).getAcquireIncrement() == 2


        cleanup:
        (ds as BoneCPDataSource).close()

    }




}
