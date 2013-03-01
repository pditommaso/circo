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

package circo.ui

/**
 * Used to render text based table for UI purpose. Example:
 *
 * <pre>
 *     def table = new TableBuilder().head('col1').head('col2').head('col3', 10)
 *     table << x << y << z << table.closeRow()
 *     table << p << q << w << table.closeRow()
 *
 *     :
 *     println table.toString()
 *     </pre>
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class TableBuilder {

    /**
     * The list defining the table header
     */
    List<TextLabel> header = []

    /**
     * All the rows
     */
    List<List<TextLabel>> allRows = []

    List<Integer> dim = []

    List<Integer> maxColsWidth = []

    String cellSeparator = ' '

    String rowSeparator = '\n'

    List<TextLabel> currentRow = []

    /**
     * Defines a single column header, for example:
     * <pre>
     *     def table = new TableBuilder().head('col1').head('col2').head('col3', 10)
     *     table << x << y << z
     *     </pre>
     *
     * @param name The string value to be used as column header
     * @param maxWidth The column max width
     * @return The table itself, to enable method chaining
     */
    TableBuilder head( String name, int maxWidth = 0 ) {
        def label = new TextLabel(name)

        header << label
        maxColsWidth << maxWidth

        def widths = header .collect { it?.toString()?.size() }
        trackWidths(widths)
        return this
    }

    /**
     * Use the specific string array as the table header definition
     * @param cols
     * @return
     */
    TableBuilder setHeader( String... cols ) {
        assert cols != null
        setHeader( cols.collect { new TextLabel(it) } as TextLabel[] )
    }

    TableBuilder setHeader( TextLabel... cols ) {
        assert cols != null
        // copy the header
        this.header = new ArrayList<>(cols as List<TextLabel>)
        // keep track of the columns width
        def widths = cols .collect { it?.toString()?.size() }
        trackWidths(widths)
        //return the object itself
        return this
    }


    TableBuilder setMaxColsWidth( int...colsWidth ) {
        assert colsWidth != null

        maxColsWidth = new ArrayList<>(colsWidth as List<Integer>)

        return this
    }

    TableBuilder append( Object... values ) {
        append( values as List )
    }

    /**
     * Append the specified list of values as the next row in the table, the columns width
     * are adapted accordingly
     *
     * @param values
     * @return
     */
    TableBuilder append( List values ) {
        assert values != null

        def row = new ArrayList<TextLabel>(values.size())
        def len = new ArrayList<Integer>(values.size())
        values.each{  it ->
            row << new TextLabel(it)
            len << it?.toString()?.size()
        }

        trackWidths(len)
        allRows << row

        this
    }

    /**
     * Defines the left-shift operator useful to build the table using the following syntax
     * <pre>
     *      def table = new TableBuilder()
     *      table << col1 << col2 << col3 << table.closeRow()
     *      :
     *     </pre>
     *
     *
     * @param value The value to be added in the table at the current row
     * @return The table instance itself
     */
    TableBuilder leftShift( def value ) {
        if( value == this ) {
            return this
        }

        if( value instanceof TextLabel ) {
            currentRow << value
        }
        else {
            currentRow << new TextLabel(value)
        }

        return this
    }

    /**
     * Close a row in the
     * @return
     */
    TableBuilder closeRow() {
        append(currentRow)
        currentRow = []
        return this
    }

    protected void trackWidths( List<Integer> newDim ) {
        def size = Math.min(dim.size(), newDim.size())

        for( int i=0; i<size; i++ ) {
            if ( dim[i] < newDim[i])  {
                 dim[i] = newDim[i]
            }
        }

        if( newDim.size() > size ) {
            for( int i=size; i<newDim.size(); i++ ) {
                dim << newDim[i]
            }
        }
    }

    /**
     * @return Render the final table and return the string
     */
    String toString() {

        // check if there's some rows no closed
        if( currentRow ) {
            closeRow()
        }


        StringBuilder result = new StringBuilder()

        def count=0

        /*
         * render the header
         */
        if( header ) {
            count++
            header.eachWithIndex { TextLabel cell, int index ->
                renderCell( result, cell, index )
            }
        }


        /*
         * render the table
         */
        allRows.each{ List<TextLabel> row ->
            // render the 'rowSeparator' (only after the first row
            if( count++ && rowSeparator!=null )  { result.append(rowSeparator) }
            // render the row
            row.eachWithIndex { TextLabel cell, int index ->
                renderCell( result, cell, index )
            }
        }

        result.toString()
    }

    /**
     * Render a cell in the table
     *
     * @param result The {@code StringBuilder} collecting the result table text
     * @param cell The cell to the rendered
     * @param index The current index in the row of the cell to be rendered
     */
    private void renderCell( StringBuilder result, TextLabel cell, int index ) {

        // the 'cellSeparator' only after the first col
        if( index && cellSeparator != null ) result.append(cellSeparator)

        // set the max col width
        if( maxColsWidth && index<maxColsWidth.size() && maxColsWidth[index] ) {
            cell.max( maxColsWidth[index] )
        }

        // set the max
        cell.width( dim[index] )

        // render the cell
        result.append( cell.toString() )

    }

}