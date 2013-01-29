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



import org.fusesource.jansi.Ansi
import org.fusesource.jansi.AnsiConsole

import static org.fusesource.jansi.Ansi.ansi

class JAnsiTest {

    public static void main(String[] args) {

        //ConsoleReader console = new ConsoleReader()
        //def height = console.getTerminal().height

        AnsiConsole.systemInstall()
        AnsiConsole.out.print Ansi.ansi().eraseScreen()
        print Ansi.ansi().a( Ansi.Attribute.BLINK_FAST )
        for( int row=1; row<= 10; row++ ) {
            print ansi().cursor(row,1).eraseLine()
            print ansi().a(Ansi.Attribute.NEGATIVE_ON)
            print "Hello world"
            print ansi().reset()
            print ansi().fg( Ansi.Color.RED )
            print "  ${row}"
            print ansi().reset()

        }

        print( ansi().reset() )
        AnsiConsole.out.flush()

        Thread.sleep(3000)

        AnsiConsole.systemUninstall();
    }
}

