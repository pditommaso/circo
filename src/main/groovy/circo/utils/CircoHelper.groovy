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

package circo.utils
import akka.actor.Address as AkkaAddress

import java.text.DecimalFormat
import java.text.DecimalFormatSymbols
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
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


}
