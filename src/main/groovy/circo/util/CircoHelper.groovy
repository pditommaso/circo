/*
 * Copyright (c) 2012, the authors.
 *
 *    This file is part of Circo.
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

package circo.util
import akka.actor.Address
import akka.actor.Address as AkkaAddress
import circo.Consts
import groovy.util.logging.Slf4j

import java.text.DecimalFormat
import java.text.DecimalFormatSymbols
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
class CircoHelper {

    static DATETIME_FORMAT = "HH:mm dd/MMM/yy"
    static SHORT_DATETIME_FORMAT = "HH:mm dd/MMM"
    static TIME_FORMAT = "HH:mm:ss"

    static final EMPTY = '-'

    static final DecimalFormatSymbols SYMBOLS = new DecimalFormatSymbols()

    static {
        SYMBOLS.setGroupingSeparator("'" as char)
    }

    static String fmt( Number value, Integer len = null ) {
        String result
        if ( !value ) {
            result = EMPTY
        }

        DecimalFormat fmt = new DecimalFormat('#,##0', SYMBOLS)
        fmt.getDecimalFormatSymbols().setGroupingSeparator(" " as char)
        result = fmt.format(value)

        if ( len != null ) {
            result = result.padLeft( len )
        }

        result
    }

    static String fmt( Date date ) {
        if( !date ) return EMPTY

        date.format(DATETIME_FORMAT)

    }


    static String fmt( AkkaAddress address, Integer pad = null ) {
        String result
        if ( !address ) {
            result = EMPTY
        }
        else {
            result = address.toString()
            int pos = result.indexOf('@')
            result = pos != 1 ? result.substring(pos+1) : result
        }

        if ( pad ) {
            result = result.padRight(pad)
        }

        result
    }


    static String getSmartTimeFormat( long millis ) {
        getSmartTimeFormat(new Date(millis))
    }

    static String getSmartTimeFormat( Date timestamp ) {
        assert timestamp

        def cal = today()

        def delta = timestamp.getTime() - cal.getTimeInMillis()
        if ( delta > 0 ) {
            timestamp.format(TIME_FORMAT)
        }
        else if ( timestamp.format('yyyy') == String.valueOf( cal.get(Calendar.YEAR)) )  {
            timestamp.format(SHORT_DATETIME_FORMAT)
        }
        else {
            timestamp.format(DATETIME_FORMAT)
        }

    }

    static Calendar today() {
        def now = Calendar.getInstance()
        def year = now.get( Calendar.YEAR )
        def month = now.get( Calendar.MONTH )
        def day = now.get( Calendar.DAY_OF_MONTH )

        new GregorianCalendar(year, month, day)
    }

    /**
     * Convert a string to an {@code Address} instance
     *
     * @param str The akka address in following the syntax {@code [protocol://system@]host:port
     * @param port The TCP port to use if it is not specified by the string value. Default {@code Consts#DEFAULT_AKKA_PORT}
     * @param system The akka system name to be used if it not specified by the string value. Default {@code Consts#DEFAULT_AKKA_SYSTEM}
     * @param protocol The akka protocol to be used if it is not specified by the string value. Default {@code Consts#DEFAULT_AKKA_PROTOCOL}
     * @return
     */
    def static Address fromString(String str, int port = Consts.DEFAULT_AKKA_PORT, String system = Consts.DEFAULT_AKKA_SYSTEM, String protocol = Consts.DEFAULT_AKKA_PROTOCOL) {
        assert str

        int p = str.indexOf('@')

        if ( p != -1 ) {
            def meta = str.substring(0,p)
            str = str.substring(p+1)


            p = meta.indexOf('://')
            if( p != -1 ) {
                protocol = meta.substring(0,p)
                system = meta.substring(p+3)
            }
            else {
                system = meta
            }
        }

        def host
        p = str.indexOf(':')
        if ( p != -1 ) {
            host = str.substring(0,p)
            port = str.substring(p+1).toInteger()
        }
        else {
            host = str
        }

        new Address(protocol,system,host,port)
    }


    static def String version(boolean full=false) {

        def result = "${Consts.APPVER}"

        if( full ) {
            result += " - ${CircoHelper.fmt(new Date(Consts.APPTIMESTAMP))}"
        }

        return result
    }

}
