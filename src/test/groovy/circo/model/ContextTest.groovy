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

package circo.model

import circo.client.CustomIntRange
import spock.lang.Specification
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class ContextTest extends Specification {

    def 'test basic operations' () {

        when:
        def context = new Context()
        context.add( new StringRef('val1','ciao') )
        context.add( new StringRef('val2', 'alpha') )
        context.add( new StringRef('val2', 'beta') )
        context.add( new StringRef('val2', 'gamma') )

        then:
        context.size() == 2
        context.getNames().sort() == ['val1','val2']
        context.getData('val1') == 'ciao'

        context.getData('val2') == ['alpha','beta','gamma']
    }

    def 'test copy ' () {

        setup:
        def context = new Context()
        context.add( new StringRef('val1','ciao') )
        context.add( new StringRef('val2', 'alpha') )
        context.add( new StringRef('val2', 'beta') )
        context.add( new StringRef('val2', 'gamma') )

        when:
        def copy = Context.copy(context)
        context.add( new StringRef('val3', 'Hi here') )
        context.add( new StringRef('val3', 'Hi there') )

        then:
        copy.size() == 2
        copy.names == ['val1','val2']
        copy.getData('val1') == context.getData('val1')
        copy.getData('val2') == context.getData('val2')
        copy.getData('val3') != context.getData('val3')
        context.getData('val3') == ['Hi here','Hi there']

    }

    def 'test equals and hashCode ' () {
        setup:
        def context = new Context()
        context.add( new StringRef('val1','ciao') )
        context.add( new StringRef('val2', 'alpha') )
        context.add( new StringRef('val2', 'beta') )
        context.add( new StringRef('val2', 'gamma') )

        when:
        def copy1 = Context.copy(context)
        def copy2 = Context.copy(context)
        copy2.add(new StringRef('x','100'))

        then:
        copy1 == context
        copy2 != context

        copy1.hashCode() == context.hashCode()
        copy2.hashCode() != context.hashCode()
    }



    def 'test add after copy ' () {

        setup:
        def context = new Context()
        context.add( new StringRef('val1','ciao') )
        context.add( new StringRef('val2', 'alpha') )
        context.add( new StringRef('val2', 'beta') )
        context.add( new StringRef('val2', 'gamma') )

        when:
        // first copy is identical
        def copy1 = Context.copy(context)

        // second copy -- when a new value is put the former value is override by the new one
        def copy2 = Context.copy(context)
        copy2.add(new StringRef('val2', 'x'))
        copy2.add(new StringRef('val2', 'y'))
        copy2.add(new StringRef('val3', 'New value'))


        then:
        copy1 == context
        copy2 != context

        // -- the first value is unchanged
        copy2.getData('val1') == context.getData('val1')
        // -- the 'val2' has been replaced by the new collection
        copy2.getData('val2') != context.getData('val2')
        copy2.getData('val2') == ['x','y']
        // -- this is a new value
        copy2.getData('val3') == 'New value'

    }

    def 'test getCollection ' () {

        setup:
        def context = new Context()
        context.add( new StringRef('val1','ciao') )
        context.add( new StringRef('val2', 'alpha') )
        context.add( new StringRef('val2', 'beta') )
        context.add( new StringRef('val2', 'gamma') )

        expect:
        context.getData('val1') == 'ciao'
        context.getData('val2')  == ['alpha','beta','gamma']
        context.getData('val9')  == new EmptyRef()


    }

    def 'test combination' () {
        setup:
        def context = new Context()
        context.add( new StringRef('val1','one') )
        context.add( new StringRef('val2', 'alpha') )
        context.add( new StringRef('val2', 'beta') )
        context.add( new StringRef('val2', 'gamma') )
        context.add( new StringRef('val3', 'x') )
        context.add( new StringRef('val3', 'y') )

        when:
        def result1 = []
        def result2 = []
        def result3 = []
        context.combinations(['val1'], { result1 << it*.data.join(',') })
        context.combinations(['val2'], { result2 << it*.data.join(',') })
        context.combinations(['val2','val3'], { result3 << it*.data.join(',') })

        then:
        result1 == ['one']
        result2 == ['alpha','beta','gamma']
        result3 == ['alpha,x','beta,x','gamma,x','alpha,y','beta,y','gamma,y']
    }

    def 'test plus operator' () {

        setup:
        def context = new Context()
        context.add( new StringRef('val1','one') )
        context.add( new StringRef('val2', 'alpha') )
        context.add( new StringRef('val2', 'beta') )
        context.add( new StringRef('val2', 'gamma') )
        context.add( new StringRef('val3', 'x') )
        context.add( new StringRef('val3', 'y') )

        when:
        def delta1 = new Context().put('p','1').put('val3','file1')
        def delta2 = new Context().put('p','1').put('q','2').put('val3','file2')
        def delta3 = new Context().put('val3','file3')

        def copy = Context.copy(context)
        copy += delta1 + delta2 + delta3

        then:
        copy.names.sort() == ['p','q','val1','val2','val3']

        // the 'val1' and 'val2' does not change
        copy.getData('val1') == 'one'
        copy.getData('val2') as Set == ['alpha','beta','gamma'].toSet()
        // 'val3' has been replaced by the delta
        copy.getData('val3') as Set == ['file1','file2','file3'].toSet()
        // these are new
        copy.getData('p') == ['1','1']
        copy.getData('q') == '2'
        copy.getData('q') != ['1','1']
        copy.getData('X') == new EmptyRef()

    }


    def 'test fromString' () {

        expect:
        Context.fromString( '[]') == []
        Context.fromString( '[ ]') == []
        Context.fromString( '[a,bb , ccc]' ) == ['a','bb','ccc']
        Context.fromString( '[a,bb , , ccc]' ) == ['a','bb','ccc']   // empty values are removed

        Context.fromString( '1..5') == [1,2,3,4,5]
        Context.fromString( 'aa..ac') == ['aa','ab','ac']
    }


    def 'test put string' () {
        setup:
        def ctx = new Context()
        ctx.add( new StringRef('val1','one') )
        ctx.add( new StringRef('val2', 'alpha') )
        ctx.add( new StringRef('val2', 'beta') )
        ctx.add( new StringRef('val2', 'gamma') )

        when:
        // replace the current value
        ctx.put('val1', 'XXX')
        ctx.put('val2', '[a,b]')
        ctx.put('val3', '1..3')
        ctx.put('val4', 'aa..ad')

        then:
        ctx.getData('val1') == 'XXX'
        ctx.getData('val2') == ['a','b']
        ctx.getData('val3') == ['1','2','3']   // <--only string values are supported right now
        ctx.getData('val4') == ['aa','ab','ac','ad']



    }


    def 'test add String' () {

        setup:
        def ctx = new Context()
        ctx.add( new StringRef('val1','one') )
        ctx.add( new StringRef('val2', 'alpha') )
        ctx.add( new StringRef('val2', 'beta') )
        ctx.add( new StringRef('val2', 'gamma') )
        ctx.add( new StringRef('val3', 'ciao') )

        when:
        // replace the current value
        ctx.add('val1', 'XXX')
        ctx.add('val2', '[a,b]')
        ctx.add('val3', '1..3')
        ctx.add('val4', 'aa..ad')

        then:
        ctx.getData('val1')  == ['one','XXX']
        ctx.getData('val2') == ['alpha','beta','gamma','a','b']
        ctx.getData('val3') == ['ciao','1','2','3']
        ctx.getData('val4') == ['aa','ab','ac','ad']

    }

    def 'test getValueAsString' () {

        expect:
        Context.itemsToString(new CustomIntRange(1,10)) == "1..10"         // basic range
        Context.itemsToString(new CustomIntRange(0,9,3)) == "0..9:3"       // range with step
        Context.itemsToString([1,3,6,8]) == "[1,3,6,8]"                    // list representation
        Context.itemsToString(['hola','hola','hola','hola']) == '[hola,..3 more]' // same repetition of the same value
        Context.itemsToString(['Hello']) == 'Hello'                        // single value is printed without brackets
        Context.itemsToString(999) == '999'
        Context.itemsToString([new FileRef('/path/name_1',1), new FileRef('/path/name_2',1)]) == '[name_1,name_2]'  // parent path are removed from files
        Context.itemsToString([new FileRef('/path/name_1',1), new FileRef('/path/name_1',1)]) == '[name_1,..1 more]'  // parent path are removed from files

    }

}
