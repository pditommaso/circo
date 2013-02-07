import groovy.text.SimpleTemplateEngine
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

class Holder implements Map {

    def Holder() {
        target = new HashMap<String,String>()
    }

    def Holder(def Map content) {
        target = new HashMap<String,String>(content)
    }

    @Delegate
    def Map target = new HashMap<>()


    Object get( String name ) {
        target.containsKey(name) ? target.get(name) : '$'+name
    }

}


// SimpleTemplateEngine.
def simple = new SimpleTemplateEngine()
def source =
    '''Dear $name,
    Please respond to this e-mail before ${(now + 7).format("dd-MM-yyyy")}
    Kind regards, $me'''

def binding = new Holder(now: new Date(109, 11, 1), name: 'Hubert Klein Ikkink')
def output = simple.createTemplate(source).make(binding).toString()

println output

//
//def tpl = """
//    Dear "\$firstname \$lastname",
//    So nice to meet you in \$city.
//    See you in \${month},
//    \${signed}
//    """
//
//def vars = [:]
//engine = new GStringTemplateEngine()
//template = engine.createTemplate(tpl).make(vars)
//println template.toString()