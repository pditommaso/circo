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
import com.mchange.v2.c3p0.ComboPooledDataSource
import com.typesafe.config.Config
import com.typesafe.config.ConfigValue
import groovy.util.logging.Slf4j

import javax.sql.DataSource
/**
 * Handles the JDBC {@code Sql} connection object
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */

@Slf4j
class JdbcDataSourceFactory {


    static def DataSource create( String url, String user='', String password='' ) {
        create( url, [user: user, password: password ] )
    }

    /**
     * Create DataSource with the provided map of properties
     * <p>
     *     http://www.mchange.com/projects/c3p0/#configuration_properties
     *
     * @param url The JDBC connection url
     * @param conf The configuration properties as defined by CP30
     * @return A pooled {code DataSource} instance
     */
    static def DataSource create( String url, Map<String,String> conf ) {

        log.debug "Creating JDBC connection pool with -- config: ${url}"
        if( !url ) {
            throw new IllegalArgumentException('Missing JDBC connection URL -- review JDBC configuration in the application.conf file')
        }

        ComboPooledDataSource cpds = new ComboPooledDataSource();

        /*
         * JDBC properties and credentials
         */
        cpds.setJdbcUrl(url)

        /*
         * pool properties
         */

        if ( conf ) {
            def properties = new Properties()
            properties.putAll(conf)
            cpds.setProperties(properties)
        }


        return cpds
    }

    /**
     * Create a JDBC Datasource using a {@code Config} as properties provider
     *
     * @param config
     * @return
     */
    static def DataSource create( Config config  ) {
        assert config
        log.debug "Creating JDBC connection pool using config: $config"

        String url = null

        url = config.getString('url')

        if( !url ) {
            throw new IllegalArgumentException('Missing JDBC connection URL -- review JDBC configuration in the application.conf file')
        }


        /*
         * Create the connection pool obj - and - set the jdbc url
         */
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setJdbcUrl(url)


        /*
         * Set the remaining connection pool properties
         * Using groovy properties syntax since {@code ComboPooledDataSource#setProperties}
         * method was not working
         */

        config.entrySet().each { entry ->
            def name = entry.key
            ConfigValue confValue = entry.value
            def value = confValue?.unwrapped()

            if( name && name != 'url') {
                try {
                    cpds."$name" = value
                }
                catch( Exception e ) {
                    if ( log.isDebugEnabled() ) {
                        log.debug("Not a valid CP30 config property: $name = $value", e)
                    }
                    else {
                        log.warn "Not a valid CP30 config property: $name = $value"
                    }
                }

            }
        }


        return cpds

    }


}
