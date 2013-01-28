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

package circo.data

import akka.actor.Address
import circo.util.SerializeId
import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString
import groovy.transform.TupleConstructor

/**
 * A reference to a generic data
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
interface DataRef extends Serializable {

    /** The name associated to this piece of data */
    def String name

    /** The grid node address where the file is allocated */
    def Address address

    /** The underlying data */
    def data

}


/**
 * Hold a reference to a file somewhere in the cluster
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@ToString
@SerializeId
@TupleConstructor
@EqualsAndHashCode
class FileRef implements DataRef {

    /** The real file on the file system */
    File file

    String name

    Address address

    def FileRef( String name, File file, Address address = null ) {
        assert name
        assert file

        this.name = name
        this.file = file
        this.address = address
    }

    def FileRef( File file, Address address = null ) {
        this(file?.name,file,address)
    }

    def FileRef( String path, Address address = null ) {
        this(new File(path),address)
    }


    def File getData() { file.absoluteFile }

}

/**
 * The simplest implementation for a data ref object
 */
@ToString
@SerializeId
@EqualsAndHashCode
class StringRef implements DataRef {

    String value

    Address address

    String name

    def StringRef( String label, def value, Address address = null ) {
        assert label
        assert value
        this.name = label
        this.value = value.toString()
        this.address = address
    }

    def String getData() { value }

}


@ToString
@SerializeId
@EqualsAndHashCode
class EmptyRef implements DataRef {

    public String getName() { throw new IllegalAccessException("Attribute 'name' is not defined for ${EmptyRef.simpleName}") }

    public String getData() { '' }
}

@ToString
@SerializeId
@EqualsAndHashCode
class ObjectRef implements DataRef {

    String name

    def data

    Address address

    ObjectRef( String name, def data ) {
        this.name = name
        this.data = data
    }
}
