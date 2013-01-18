/*
 * Copyright (c) 2012, the authors.
 *
 *    This file is part of RUSH.
 *
 *    Moke is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation, either version 3 of the License, or
 *    (at your option) any later version.
 *
 *    Moke is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with RUSH.  If not, see <http://www.gnu.org/licenses/>.
 */

package rush.data

import com.mchange.v2.c3p0.ComboPooledDataSource
import com.typesafe.config.ConfigFactory
import spock.lang.Specification

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class JdbcDataSourceFactoryTest extends Specification {

    def 'test create' () {

        when:
        def ds = JdbcDataSourceFactory.create( 'jdbc:h2:mem:Rush' )

        then:
        noExceptionThrown()
        !ds.getConnection().isClosed()

    }

    def 'test create with map ' () {

        when:
        def ds = JdbcDataSourceFactory.create( 'jdbc:h2:mem:Rush', [user:'paolo', password:'ciao'] )

        then:
        noExceptionThrown()
        !ds.getConnection().isClosed()
        ds instanceof ComboPooledDataSource
        (ds as ComboPooledDataSource).getUser() == 'paolo'
        (ds as ComboPooledDataSource).getPassword() == 'ciao'

    }


    def 'test CreateWithConfig' () {

        setup:
        def str = """
        store {
            jdbc {
              url = "jdbc:h2:mem:xxx"
              user = paolo
              password = zzz
              minPoolSize = 1
              maxPoolSize = 10
              acquireIncrement = 1
            }
        }
        """

        def conf = ConfigFactory.parseString( str )

        when:
        def ds = JdbcDataSourceFactory.create( conf.getConfig('store.jdbc') )

        then:
        noExceptionThrown()
        !ds.getConnection().isClosed()
        ds instanceof ComboPooledDataSource
        (ds as ComboPooledDataSource).getUser() == 'paolo'
        (ds as ComboPooledDataSource).getPassword() == 'zzz'
        (ds as ComboPooledDataSource).getAcquireIncrement() == 1
        (ds as ComboPooledDataSource).getMinPoolSize() == 1
        (ds as ComboPooledDataSource).getMaxPoolSize() == 10

    }




}
