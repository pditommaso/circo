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
class Const {

    static final File APP_HOME_DIR

    static final File APP_TMP_DIR

    static {
        APP_HOME_DIR = new File( System.getProperty("user.home"), ".${Const.APP_NAME}" )
        if( !APP_HOME_DIR.exists() && !APP_HOME_DIR.mkdir() ) {
            throw new IllegalStateException("Cannot create path '${APP_HOME_DIR}' -- check file system access permission")
        }

        APP_TMP_DIR = new File(APP_HOME_DIR, 'tmp')
        if( !APP_TMP_DIR.exists() && !APP_TMP_DIR.mkdirs()) {
            throw new IllegalStateException("Cannot create path '${APP_TMP_DIR}' -- check file system access permission")
        }
    }

    /**
     * The ascii art logo, displayed on start
     * Made with font 'chunky' at http://www.network-science.de/ascii/
     */
    def static LOGO = """\
     ______ __
    |      |__|.----.----.-----.
    |   ---|  ||   _|  __|  _  |
    |______|__||__| |____|_____|  ver. ${APP_VER}

    """
    .stripIndent()

    static final String MAIN_PACKAGE = Const.class.name.split('\\.')[0]

    static final String APP_NAME = MAIN_PACKAGE

    static final String APP_VER = "0.3.0"

    static final long APP_TIMESTAMP = 1360319471477

    static final int APP_BUILDNUM = 327

    static final String LOCAL_ADDRESS = '127.0.0.1'

    static final int DEFAULT_AKKA_PORT = 2551

    static final String DEFAULT_AKKA_PROTOCOL = 'akka'

    static final String DEFAULT_AKKA_SYSTEM = 'circo'

    static final LOCAL_NAMES = ['this','local','localhost']

    public static final LIST_OPEN_BRACKET = '['

    public static final LIST_CLOSE_BRACKET = ']'

}
