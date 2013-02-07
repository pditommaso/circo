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

package circo.ui

import akka.actor.UntypedActor
import akka.cluster.Cluster
import circo.data.DataStore
import groovy.util.logging.Slf4j
import jline.console.ConsoleReader
import org.fusesource.jansi.Ansi
import org.fusesource.jansi.AnsiConsole
import scala.concurrent.duration.FiniteDuration
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
class TerminalUI extends UntypedActor {

    final static ACTOR_NAME = 'TerminalUI'

    protected final TICK = 'TICK'

    protected final int nodeId

    protected Cluster cluster

    protected DataStore dataStore

    protected ScreenRenderer previous

    protected ConsoleReader console

    protected int height

    protected int width

    protected Closure highlighter


    def TerminalUI( DataStore dataStore, int nodeId ) {
        this.dataStore = dataStore
        this.nodeId = nodeId
    }


    @Override
    def void preStart() {
        log.debug "++ Starting actor ${getSelf().path()}"

        if ( getContext().system().isTerminated() ) { return }

        cluster = Cluster.get(getContext().system())

        console = new ConsoleReader()
        AnsiConsole.systemInstall()
        AnsiConsole.out.print Ansi.ansi().eraseScreen()

        highlighter = { value ->
            if( value instanceof TextLabel ) value.switchOn()
            else { TextLabel.of(value).bold().switchOn() }
        }

        // start the scheduler to update the screen
        def twoSecs = FiniteDuration.create('2 seconds') as FiniteDuration
        getContext().system().scheduler().schedule( FiniteDuration.Zero(), twoSecs, getSelf(), TICK, getContext().dispatcher() )

    }

    @Override
    def void postStop() {
        log.debug "-- Stopping actor ${getSelf().path()}"
        AnsiConsole.out.print Ansi.ansi().eraseScreen().reset()
        AnsiConsole.out.flush()
        AnsiConsole.systemUninstall()
    }

    @Override
    def void onReceive(Object message) {

        if ( message == TICK) {
            height = console.getTerminal().height
            width = console.getTerminal().width

            def lines = renderScreen()
            refreshScreen(lines)
        }
    }

    StringBuilder renderScreen(Closure highlight = null) {

        def block = new ScreenRenderer(nodeId, dataStore)

        try {
           if( previous ) {
               block.compareWith(previous, highlighter)
           }
           return block.render()
        }
        finally {
            block.noCompare()
            previous = block
        }

    }


    void refreshScreen( StringBuilder lines ) {

        def row = 1
        lines.eachLine { line ->
            AnsiConsole.out.print Ansi.ansi().cursor(row++,1).eraseLine()
            AnsiConsole.out.print line.trim()
        }

        AnsiConsole.out.flush()
    }





}
