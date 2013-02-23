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

package circo.client

import circo.model.EmptyRef
import circo.model.Context
import com.beust.jcommander.JCommander
import spock.lang.Shared
import spock.lang.Specification

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class CmdContextTest extends Specification {

    @Shared def Context ctx

    @Shared def CmdContext cmd

    @Shared def JCommander parser

    def setup() {
        ctx = new Context().add('X','1').add('X','2')
        cmd = new CmdContext()
        parser = new JCommander(cmd)
    }


    def 'test get' () {

        when:
        parser.parse('--get', 'X')

        then:
        cmd.apply(ctx) == '[1,2]'

    }


    def 'test set' () {

        when:
        parser.parse('--set', 'X=9')
        cmd.apply(ctx)

        then:
        ctx.getData('X') == '9'

    }

    def 'test add' () {

        when:
        parser.parse('--add', 'X=9')
        cmd.apply(ctx)

        then:
        ctx.getData('X') == ['1','2','9']

    }

    def 'test unset' () {

        when:
        parser.parse('--unset', 'X')
        cmd.apply(ctx)

        then:
        ctx.getData('X') == new EmptyRef()

    }


    def 'test empty' () {


        when:
        assert !ctx.isEmpty()
        parser.parse('--empty')
        cmd.apply(ctx)

        then:
        ctx.isEmpty()

    }

    def 'test import' () {

        setup:
        def file = new File('$$test.context')
        file.text = '''
        A=1
        B=[x,y,z]
        C=0..3
        '''

        when:
        parser.parse('--import', file.name)
        cmd.apply(ctx)

        then:
        ctx.getData('A') == '1'
        ctx.getData('B') == ['x','y','z']
        ctx.getData('C') == ['0','1','2','3']

        cleanup:
        file.delete()

    }

    def 'test import ENV' () {

        setup:
        def env = new HashMap<String,String>()
        env['X_1'] = 'uno'
        env['X_2'] = 'due'
        env['X_3'] = 'tre'

        when:
        def count = ctx.size()
        parser.parse('--import', 'env')
        cmd.apply(ctx, env)

        then:
        ctx.size() == 3 + count
        ctx.getData('X_1') == 'uno'
        ctx.getData('X_2') == 'due'
        ctx.getData('X_3') == 'tre'
    }

    def 'test import ENV with grep' () {

        setup:
        def env = new HashMap<String,String>()
        env['X_1'] = 'uno'
        env['X_2'] = 'due'
        env['X_3'] = 'tre'
        env['Z'] = '9'

        when:
        def count = ctx.size()
        parser.parse('--import', 'env', '--grep', 'Z.*')
        cmd.apply(ctx, env)

        then:
        ctx.size() == 1 + count
        ctx.getData('Z')  == '9'

    }

}
