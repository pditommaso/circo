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
import ch.qos.logback.classic.Level
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.filter.Filter
import ch.qos.logback.core.spi.FilterReply
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class PackageFilter extends Filter<ILoggingEvent> {

    Map<String,Level> packages

    PackageFilter( Map<String,Level> packages )  {
        this.packages = packages
    }

    @Override
    FilterReply decide(ILoggingEvent event) {

        if (!isStarted()) {
            return FilterReply.NEUTRAL;
        }

        def logger = event.getLoggerName()
        def level = event.getLevel()
        for( def entry : packages ) {
            if ( logger.startsWith( entry.key ) && level.isGreaterOrEqual(entry.value) ) {
                return FilterReply.NEUTRAL
            }
        }

        return FilterReply.DENY
    }
}
