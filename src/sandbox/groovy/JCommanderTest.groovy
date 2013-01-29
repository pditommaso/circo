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



import com.beust.jcommander.JCommander
import circo.client.cmd.AppOptions
import circo.client.cmd.CmdNode
import circo.client.cmd.CmdStat
import circo.client.cmd.CmdSub
import spock.lang.Specification
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class JCommanderTest extends Specification {

    def testParseCli() {

        setup:
        def cli = new AppOptions()

        def jcommander = new JCommander(cli)
        jcommander.addCommand( new CmdSub() )
        jcommander.addCommand( new CmdNode() )
        jcommander.addCommand( new CmdStat() )

        when:
        jcommander.parse('stat', '1', '2', '3', 'list')
        println jcommander.getCommands().get( jcommander.parsedCommand ).objects

        then:
        jcommander.parsedCommand == 'job'

    }

//
//    def 'test variable arity' ( ) {
//
//        when:
//        def bean = new VarArity()
//        def jc = new JCommander(bean)
//        jc.parse('-f')
//
//        then:
//        bean.field == []
//
//    }
//
//
//    static class VarArity {
//
//        @Parameter(names = '-f', listConverter = VarConverter)
//        def field
//
//    }
//
//    static class VarConverter implements IStringConverter<List<String> > {
//
//        @Override
//        List<String> convert(String value) {
//
//            if ( !value ) return []
//
//            value.split(',')
//        }
//    }
}
