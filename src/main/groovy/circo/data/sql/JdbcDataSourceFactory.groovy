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
import javax.sql.DataSource

import com.jolbox.bonecp.BoneCPDataSource
import com.typesafe.config.Config
import com.typesafe.config.ConfigValue
import groovy.util.logging.Slf4j


/**
 * JDBC {@code DataSource} connection factory, it uses the BoneCP connection pool
 * <p> See http://jolbox.com
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */

@Slf4j
class JdbcDataSourceFactory {

    static DataSource instance


    static def DataSource create( String url, String user='', String password='' ) {
        instance = create( url, [username: user, password: password ] )
    }

    /**
     * Create DataSource with the provided map of properties
     * <p>
     *     http://jolbox.com/index.html?page=http://jolbox.com/configuration.html
     *
     * @param url The JDBC connection url
     * @param conf The configuration properties as defined by BonceCP
     * @return A pooled {code DataSource} instance
     */
    static def DataSource create( String url, Map<String,Object> conf ) {

        log.debug "Creating JDBC connection pool with -- config: ${url}"
        if( !url ) {
            throw new IllegalArgumentException('Missing JDBC connection URL -- review JDBC configuration in the application.conf file')
        }

        BoneCPDataSource dataSource = new BoneCPDataSource();
        dataSource.setJdbcUrl(url);

        /*
         * set configuration properties
         */
        conf.each { String name, Object value ->
            try {
                dataSource[name] = value
            }
            catch( Exception e ) {

                if ( log.isDebugEnabled() ) {
                    log.debug("Not a valid CP30 config property: '$name' = '$value'", e)
                }
                else {
                    log.warn "Not a valid CP30 config property: '$name' = '$value'"
                }

            }
        }

        instance = dataSource
    }

    /**
     * Create a JDBC Data-source using a {@code Config} as properties provider
     * <p>
     *     Available properties as defined by
     *     http://jolbox.com/index.html?page=http://jolbox.com/configuration.html
     *
     *
     * @param config
     * @return
     */
    static def DataSource create( Config config ) {
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
        BoneCPDataSource dataSource = new BoneCPDataSource();
        dataSource.setJdbcUrl(url);


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
                    dataSource[name] = value
                }
                catch( Exception e ) {
                    if ( log.isDebugEnabled() ) {
                        log.debug("Not a valid CP30 config property: '$name' = '$value'", e)
                    }
                    else {
                        log.warn "Not a valid CP30 config property: '$name' = '$value'"
                    }
                }

            }
        }


        instance = dataSource

    }


}
