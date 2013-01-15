/*
 * Copyright (c) 2012, the authors.
 *
 *    This file is part of Rush.
 *
 *    Rush is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation, either version 3 of the License, or
 *    (at your option) any later version.
 *
 *    Rush is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with Rush.  If not, see <http://www.gnu.org/licenses/>.
 */

package rush.utils
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class RushHelper {

    static DATETIME_FORMAT = "HH:mm dd/MMM/yy"
    static SHORT_DATETIME_FORMAT = "HH:mm dd/MMM"
    static TIME_FORMAT = "HH:mm:ss"

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
