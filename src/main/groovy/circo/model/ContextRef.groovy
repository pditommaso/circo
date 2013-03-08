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
import circo.data.DataStore
import circo.util.SerializeId
import groovy.transform.EqualsAndHashCode
import groovy.transform.TupleConstructor
/**
 * A reference to a generic data
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
interface DataRef extends Serializable {

    /** The name associated to this piece of data */
    def String name

    /** The underlying data */
    def data

}


/**
 * Hold a reference to a file somewhere in the cluster
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@SerializeId
@TupleConstructor
@EqualsAndHashCode
class FileRef implements DataRef {

    /**
     * A reference to the current data store provider. This is injected at system bootstrap
     */
    static DataStore dataStore

    /**
     * The current cluster node identifier
     */
    static int currentNodeId

    /**
     * The file name, not including the local path structure
     */
    final String name

    final UUID fileId

    final String fileName

    transient File localFile

    /**
     * Create a reference for a file system file
     * @param file
     * @param nodeId
     * @param name
     * @return
     */
    def FileRef( File file, String name ) {
        assert name != null
        assert file

        // the FileRef 'variable' name
        this.name = name

        // the reference to the local version of the file -- not this is a transient variable
        this.localFile = file

        // unique file it
        this.fileId = UUID.randomUUID()

        // local file name -- note: it may contains relative path to the current folder
        this.fileName = file.name.toString()

        // save in the cluster storage
        if ( file.exists() ) {
            dataStore.saveFile( fileId, file )
        }

    }

    def FileRef( File file ) {
        this(file, file?.name)
    }

    def FileRef( String path ) {
        this(new File(path) )
    }


    def File getData() {

        if ( localFile ) {
            return localFile
        }

        localFile = dataStore.getFile(fileId)
        if ( !localFile ) {
            throw new IllegalStateException("Missing cache store file: ${localFile.toString()}")
        }

        return localFile
    }


    @Override
    String toString() { fileName }

}

/**
 * The simplest implementation for a data ref object
 */
@SerializeId
@EqualsAndHashCode
class StringRef implements DataRef {

    String name

    String value

    def StringRef( String label, def value ) {
        assert label != null
        assert value != null

        this.name = label
        this.value = value.toString()
    }

    def String getData() { value }

    @Override
    String toString() { value }


}


@SerializeId
@EqualsAndHashCode
class EmptyRef implements DataRef {

    public String getName() { throw new IllegalAccessException("Attribute 'name' is not defined for ${EmptyRef.simpleName}") }

    public String getData() { '' }

    public String toString() { '' }
}

@SerializeId
@EqualsAndHashCode
class ObjectRef implements DataRef {

    String name

    def data

    ObjectRef( String name, def data ) {
        this.name = name
        this.data = data
    }

    String toString() { data?.toString() }
}
