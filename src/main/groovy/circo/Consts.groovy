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

package circo

/**
 * Application constants
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class Consts {

    /**
     * The ascii art logo, displayed on start
     */
    def static LOGO = """\
       ___ _
      / __(_)_ _ __ ___
     | (__| | '_/ _/ _ \\
      \\___|_|_| \\__\\___/   ver ${APPVER}

    """
    .stripIndent()

    static final String MAIN_PACKAGE = Consts.class.name.split('\\.')[0]

    static final String APPNAME = MAIN_PACKAGE

    static final String APPVER = "0.1.3"

    static final long APPTIMESTAMP = 1359066495511

    static final String LOCAL_ADDRESS = '127.0.0.1'

    static final int DEFAULT_AKKA_PORT = 2551

    static final String DEFAULT_AKKA_PROTOCOL = 'akka'

    static final String DEFAULT_AKKA_SYSTEM = 'circo'

    static final LOCAL_NAMES = ['this','local','localhost']


}
