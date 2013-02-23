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
import circo.util.CircoHelper
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

    final UUID cacheKey

    final File file

    /**
     * A map holding the absolute path where the file is stored on the local file system on each cluster node
     */
    final Map<Integer,File> localFile = [:]

    /**
     * Create a reference for a file system file
     * @param file
     * @param nodeId
     * @param name
     * @return
     */
    def FileRef( File file, int nodeId, String name ) {
        assert name != null
        assert file

        this.file = file
        this.name = name
        this.localFile[nodeId] = file.absoluteFile
        this.cacheKey = UUID.randomUUID()

        // save in the cluster storage
        if ( file.exists() ) {
            def channel = new FileInputStream(file)?.getChannel()
            dataStore.putFile( cacheKey.toString(), channel )
            channel.close()
        }

    }

    def FileRef( File file, int nodeId ) {
        this(file, nodeId, file?.name)
    }

    def FileRef( String path, int nodeId ) {
        this(new File(path), nodeId)
    }


    def File getData() {

        // -- check if the file is available on the local file system
        if ( currentNodeId in localFile ) {
            return localFile[ currentNodeId ]
        }

        // -- the file is not available in node local file system
        //    try to retrieve on the cluster cache
        def result = new File( CircoHelper.createScratchDir(), name )
        def target = new FileOutputStream(result).getChannel()

        target = dataStore.getFile(cacheKey.toString(), target)
        if ( !target ) {
            result.delete()
            throw new IllegalStateException("Missing cache store file: ${file.toString()}")
        }
        target.close()

        // -- keep this file on the local cache
        localFile[currentNodeId] = result

        return result
    }


    @Override
    String toString() { file?.toString() }

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
