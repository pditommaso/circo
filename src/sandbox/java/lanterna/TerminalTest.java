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

package lanterna;

import java.nio.charset.Charset;

import com.googlecode.lanterna.TerminalFacade;
import com.googlecode.lanterna.input.Key;
import com.googlecode.lanterna.screen.Screen;
import com.googlecode.lanterna.screen.ScreenWriter;
import com.googlecode.lanterna.terminal.text.UnixTerminal;

/**
 *
 * @author martin
 */
public class TerminalTest
{
    public static void main(String[] args) {

        final Charset DEFAULT_CHARSET = Charset.forName(System.getProperty("file.encoding"));

        final Screen screen = TerminalFacade.createScreen( new UnixTerminal(System.in,System.out, DEFAULT_CHARSET, null, UnixTerminal.Behaviour.CTRL_C_KILLS_APPLICATION));
        ScreenWriter writer = new ScreenWriter(screen);
        screen.startScreen();

        Thread t = new Thread() {

            @Override
            public void run() {
                while( true ) {
                    Key key = screen.getTerminal().readInput();
                    if(key == null) {
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException e) {
                            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                        }
                        continue;
                    }


                    if( key !=null && key.isCtrlPressed() && key.getCharacter() == 'c' ) {
                        screen.stopScreen();
                        System.exit(0);
                    }
                }
            }
        };
        t.start();


        try {
            Thread.sleep(500);
        }
        catch(InterruptedException e) {}
        writer.drawString(10, 10, "Hello world!");
        screen.refresh();
        try {
            Thread.sleep(500);
        }
        catch(InterruptedException e) {}
        writer.drawString(10, 10, " ello world!");
        screen.refresh();
        try {
            Thread.sleep(500);
        }
        catch(InterruptedException e) {}
        writer.drawString(10, 10, "  llo world!");
        screen.refresh();
        try {
            Thread.sleep(500);
        }
        catch(InterruptedException e) {}
        writer.drawString(10, 10, "   lo world!");
        screen.refresh();
        try {
            Thread.sleep(500);
        }
        catch(InterruptedException e) {}
        writer.drawString(10, 10, "    o world!");
        screen.refresh();
        try {
            Thread.sleep(500);
        }
        catch(InterruptedException e) {}
        writer.drawString(10, 10, "      world!");
        screen.refresh();
        try {
            Thread.sleep(500);
        }
        catch(InterruptedException e) {}
        writer.drawString(10, 10, "       orld!");
        screen.refresh();
        try {
            Thread.sleep(500);
        }
        catch(InterruptedException e) {}
        writer.drawString(10, 10, "        rld!");
        screen.refresh();
        try {
            Thread.sleep(500);
        }
        catch(InterruptedException e) {}
        writer.drawString(10, 10, "         ld!");
        screen.refresh();
        try {
            Thread.sleep(500);
        }
        catch(InterruptedException e) {}
        writer.drawString(10, 10, "          d!");
        screen.refresh();
        try {
            Thread.sleep(500);
        }
        catch(InterruptedException e) {}
        writer.drawString(10, 10, "           !");
        screen.refresh();
        try {
            Thread.sleep(500);
        }
        catch(InterruptedException e) {}
        writer.drawString(10, 10, "            ");
        screen.refresh();
        try {
            Thread.sleep(500);
        }
        catch(InterruptedException e) {}
        screen.stopScreen();
    }
}