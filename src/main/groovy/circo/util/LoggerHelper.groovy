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
import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.ConsoleAppender
import ch.qos.logback.core.CoreConstants
import ch.qos.logback.core.LayoutBase
import ch.qos.logback.core.encoder.LayoutWrappingEncoder
import ch.qos.logback.core.filter.Filter
import ch.qos.logback.core.rolling.RollingFileAppender
import ch.qos.logback.core.rolling.TimeBasedRollingPolicy
import ch.qos.logback.core.spi.FilterReply
import circo.Consts
import org.slf4j.LoggerFactory
/**
 * Helper methods to setup the logging subsystem
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class LoggerHelper {


    static class CliLoggingArgs {

        List<String> debug
        List<String> trace
        List<String> opts

        static CliLoggingArgs parse( String[] args ) {

            def defaultPackage = LoggerHelper.getName().split('\\.')[0]

            List<String> debug = null
            List<String> trace = null

            List<String> opts = new ArrayList<>(args as List<String>)

            int p
            if( (p=opts.indexOf('--debug')) != -1 ) {
                // remove the debug options
                opts.remove(p)
                debug = []

                // looks for optional arguments
                while( p < opts.size() && !opts[p].startsWith('-') ) {
                    debug.addAll( opts[p].split(",") )
                    opts.remove(p)
                }

                if ( !debug ) debug << defaultPackage
            }

            if( (p = (args as List).indexOf('--trace'))!=-1 ) {
                // remove the trace options
                opts.remove(p)
                trace = []

                // looks for optional arguments
                while( p < opts.size() && !opts[p].startsWith('-') ) {
                    trace.addAll( opts[p].split(",") )
                    opts.remove(p)
                }

                if ( !trace ) opts << defaultPackage
            }

            // return as an object
            new CliLoggingArgs( debug: debug, trace: trace, opts: opts )
        }

    }


    /**
     * Configure the client application logging subsytem.
     * <p>
     *     It looks on the cli argument for the following options
     *     <li>--debug
     *     <li>--trace
     *
     * <p>
     *     On both of them can be optionally specified a comma separated list of packages to which
     *     apply the specify logging level
     *
     *
     * @param args The app cli arguments as entered by the user.
     */
    static List<String> configureClientLogger(String[] args) {

        def logConf = CliLoggingArgs.parse(args)

        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory()

        // Reset all the logger
        def root = loggerContext.getLogger('ROOT')
        root.detachAndStopAllAppenders()

        // -- define the console appender
        Map<String,Level> packages = [:]
        packages[Consts.MAIN_PACKAGE] = Level.INFO
        packages['akka'] = Level.WARN
        logConf.debug?.each { packages[it] = Level.DEBUG }
        logConf.trace?.each { packages[it] = Level.TRACE }

        def filter = new LoggerPackageFilter( packages )
        filter.setContext(loggerContext)
        filter.start()

        def consoleAppender = new ConsoleAppender()
        consoleAppender.setContext(loggerContext)
        consoleAppender.setEncoder( new LayoutWrappingEncoder( layout: new PrettyConsoleLayout() ) )
        consoleAppender.addFilter(filter)
        consoleAppender.start()

        // -- the file appender
        def fileName = ".${Consts.MAIN_PACKAGE}.log"
        def fileAppender = new RollingFileAppender()
        def timeBasedPolicy = new TimeBasedRollingPolicy( )
        timeBasedPolicy.fileNamePattern = "${fileName}.%d{yyyy-MM-dd}"
        timeBasedPolicy.setContext(loggerContext)
        timeBasedPolicy.setParent(fileAppender)
        timeBasedPolicy.start()

        def encoder = new PatternLayoutEncoder()
        encoder.setPattern('%d{HH:mm:ss.SSS} %-5level %logger{36} - %msg%n')
        encoder.setContext(loggerContext)
        encoder.start()

        fileAppender.file = fileName
        fileAppender.rollingPolicy = timeBasedPolicy
        fileAppender.encoder = encoder
        fileAppender.setContext(loggerContext)
        fileAppender.start()

        // -- configure the ROOT logger
        root.setLevel(Level.INFO)
        root.addAppender(fileAppender)
        root.addAppender(consoleAppender)

        // -- main package logger
        def mainLevel = packages[Consts.MAIN_PACKAGE]
        def logger = loggerContext.getLogger(Consts.MAIN_PACKAGE)
        logger.setLevel( mainLevel == Level.TRACE ? Level.TRACE : Level.DEBUG )
        logger.setAdditive(false)
        logger.addAppender(fileAppender)
        logger.addAppender(consoleAppender)


        // -- debug packages specified by the user
        logConf.debug?.each { String clazz ->
            logger = loggerContext.getLogger( clazz )
            logger.setLevel(Level.DEBUG)
            logger.setAdditive(false)
            logger.addAppender(fileAppender)
            logger.addAppender(consoleAppender)
        }

        // -- trace packages specified by the user
        logConf.trace?.each { String clazz ->
            logger = loggerContext.getLogger( clazz )
            logger.setLevel(Level.TRACE)
            logger.setAdditive(false)
            logger.addAppender(fileAppender)
            logger.addAppender(consoleAppender)
        }


        // return all but the 'debug' and 'trace' options
        return logConf.opts
    }

    /**
     * Configure the SL4J logging subsystem based the CLI arguments specified by the user.
     *
     * Basically when the instance run in the 'interactive' mode the logging is redirected to
     * a file (RollingAppender) - otherwise the logging goes to te console
     *
     * @param cmdLine
     */
    static void configureDaemonLogger(CmdLine cmdLine) {

        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory()

        // Reset all the logger
        def root = loggerContext.getLogger('ROOT')
        root.detachAndStopAllAppenders()

        def encoder = new PatternLayoutEncoder()
        encoder.setPattern('%d{HH:mm:ss.SSS} %-5level %logger{36} - %msg%n')
        encoder.setContext(loggerContext)
        encoder.start()

        def appender
        if( !cmdLine.interactive ) {
            appender = new ConsoleAppender()
            appender.setContext(loggerContext)
            appender.setEncoder(encoder)
            appender.start()
        }
        else {

            appender = new RollingFileAppender()

            def fileName = ".${Consts.APPNAME}-daemon-${cmdLine.port}.log"
            def timeBasedPolicy = new TimeBasedRollingPolicy( )
            timeBasedPolicy.fileNamePattern = "${fileName}.%d{yyyy-MM-dd}"
            timeBasedPolicy.setContext(loggerContext)
            timeBasedPolicy.setParent(appender)
            timeBasedPolicy.start()

            appender.file = fileName
            appender.rollingPolicy = timeBasedPolicy
            appender.encoder = encoder
            appender.setContext(loggerContext)
            appender.start()

        }

        Logger logger = loggerContext.getLogger('ROOT')
        logger.setLevel(Level.INFO)
        logger.addAppender(appender)

        logger = loggerContext.getLogger( Consts.MAIN_PACKAGE )
        logger.setLevel(Level.DEBUG)
        logger.setAdditive(false)
        logger.addAppender(appender)

        def roots = ['root', 'true', '']

        cmdLine.debug?.each { String it ->
            def clazz = (it.toLowerCase() in roots) ? 'ROOT' : it
            logger = loggerContext.getLogger( clazz )
            logger.setLevel(Level.DEBUG)
            logger.setAdditive(false)
            logger.addAppender(appender)
        }

        cmdLine.trace?.each { String it ->

            def clazz = (it.toLowerCase() in roots) ? 'ROOT' : it
            logger = loggerContext.getLogger( clazz )
            logger.setLevel(Level.TRACE)
            logger.setAdditive(false)
            logger.addAppender(appender)

        }

    }

    /*
     * Filters the logging event based on the level assigned to a specific 'package'
     */
    static class LoggerPackageFilter extends Filter<ILoggingEvent> {

        Map<String,Level> packages

        LoggerPackageFilter( Map<String,Level> packages )  {
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

    /*
     * Do not print INFO level prefix, used to print logging information
     * to the application stdout
     */
    static class PrettyConsoleLayout extends LayoutBase<ILoggingEvent> {

        public String doLayout(ILoggingEvent event) {
            StringBuilder buffer = new StringBuilder(128);
            if( event.getLevel() != Level.INFO ) {
                buffer.append( event.getLevel().toString() ) .append(": ")
            }

            return buffer
                    .append(event.getFormattedMessage())
                    .append(CoreConstants.LINE_SEPARATOR)
                    .toString()
        }
    }

}
