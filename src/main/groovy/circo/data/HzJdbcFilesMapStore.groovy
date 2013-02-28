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
import java.sql.Blob

import circo.util.CircoHelper
import groovy.transform.InheritConstructors
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@InheritConstructors
class HzJdbcFilesMapStore extends AbstractHzJdbcMapStore<UUID, File> {


    @Override
    String getTableName() { "FILE_DATA" }

    @Override
    def void createTable( ) {
        assert sql

        sql.execute """
            SET COMPRESS_LOB = LZF;
            create table if not exists ${tableName} (
              ID VARCHAR PRIMARY KEY,
              NAME VARCHAR,
              CONTENT BLOB
            );

        """.toString()
    }

    @Override
    Object keyToObj(UUID key) { key.toString() }

    @Override
    UUID objToKey(value) { UUID.fromString(value) }

    @Override
    def File empty(UUID key) { null }

    /**
     * Store a local file into the shared DB
     *
     * @param fileId The file UUID identifying it uniquely
     * @param file A file local stored in the local file system
     */
    @Override
    void store(UUID fileId, File file) {
        assert fileId
        assert file

        def merge = "MERGE INTO $tableName (ID, NAME, CONTENT) VALUES (?,?,?)" .toString()
        sql.execute( merge , [ fileId.toString(), file.name, new FileInputStream(file) ])

    }

    /**
     * Extract a file from the db and store to a file in local file system, so that
     * it can be accessed by other programs
     *
     * @param fileId The file unique UUID
     * @return A file stored in the local fs or {@code null} if the file with the specified ID does not exist
     */
    @Override
    File load( UUID fileId ) {
        assert fileId

        def statement = "select NAME, CONTENT from ${tableName} where ID=?".toString()
        def row = sql.firstRow(statement, fileId.toString())

        if ( !row ) {
            return null
        }

        def name = row[0] as String
        def binary = (row[1] as Blob)?.getBinaryStream()

        if( !name ) {
            throw new IllegalAccessException("Missing 'name' for file with id: $fileId")
        }

        // create a temporary file to hold it
        File result = new File( CircoHelper.createScratchDir(), name )
        FileOutputStream out = new FileOutputStream(result)

        try {
            int c
            byte[] buffer = new byte[8_000]
            while( (c = binary.read(buffer)) != -1 ) {
                out.write(buffer,0,c)
            }

        }
        finally {
            out.close()
        }

        return result
    }

    /**
     * Operation not supported
     */
    @Override
    void storeAll(Map<UUID, File> map) {

        throw new IllegalAccessException('Operation not supported')

    }

    /**
     * Operation not supported
     */
    Map<UUID, File> loadAll(Collection<UUID> keys) {

        throw new IllegalAccessException('Operation not supported')

    }



}
