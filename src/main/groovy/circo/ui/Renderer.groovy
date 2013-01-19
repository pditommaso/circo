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

package circo.ui

import akka.actor.Address
import groovy.transform.EqualsAndHashCode
import circo.data.DataStore
import circo.data.NodeData
import circo.data.WorkerData
import circo.messages.JobEntry
import org.fusesource.jansi.Ansi

import static org.fusesource.jansi.Ansi.ansi

/**
 * Holds the cluster data
 */

class ClusterRenderer extends DataHolder {

    public ClusterRenderer() {

        this.numOfNodes = 0
        this.processedJobs = 0
        this.failedJobs = 0

    }

    def ClusterRenderer( List<NodeData> nodes ) {

        long processed = 0
        long failed = 0
        nodes.each { NodeData data ->
            processed += data.processed
            failed += data.failed
        }

        this.numOfNodes = TextLabel.of(nodes.size())
        this.processedJobs = TextLabel.of(processed)
        this.failedJobs = TextLabel.of(failed) << AnsiStyle.error()
    }


    StringBuilder render( StringBuilder builder ) {
        builder <<
        """
        Cluster status
        --------------
        Nodes: ${numOfNodes} - Jobs: ${allJobs} - Processed: ${processedJobs} - Failed: ${failedJobs}

        """
        .stripIndent()

    }

}

/**
 * Holds the Node data
 */
class NodeRender extends DataHolder {


    def NodeRender( NodeData data ) {

        address = data.address?.toString()
        startTime = new Date(data.startTimestamp).format('H:mm:ss dd MMM yyyy')
        processed = TextLabel.of(data.processed)
        failed = TextLabel.of(data.failed) << AnsiStyle.error()
        queueSize = TextLabel.of(data.queue.size())

    }

    def StringBuilder render( StringBuilder builder ) {

        builder <<
        """\
        Node [${address}] - ${startTime}
        -----------------
        Queued: ${queueSize} - Processed: ${processed} - Failed: ${failed}
        """
            .stripIndent()

        return builder
    }

}

/**
 * Holds a single worker (row) data
 */
class WorkerRenderer extends DataHolder {


    def WorkerRenderer(WorkerData data, JobEntry jobEntry) {
        assert data

        this.worker = TextLabel.of(data.worker.path().name()).pad(15)
        this.jobId = TextLabel.of(data.currentJobId?.toFmtString()).pad(5).right()
        this.processed = TextLabel.of(data.processed).number().pad(5)
        this.failed = TextLabel.of(data.failed).pad(3).number()
        this.jobAttempts = TextLabel.of(jobEntry?.attempts).number().pad(5) << AnsiStyle.error()
    }


    def StringBuilder render( StringBuilder builder ) {

        builder <<  "${worker} ${processed} / ${failed}   ${jobId} ${jobAttempts}"

    }

}


/**
 * Holds all the data to the rendered in the monitor screen
 */
class ScreenRenderer extends DataHolder {

    def ClusterRenderer cluster
    def NodeRender node
    def List<WorkerRenderer> workers

    def ScreenRenderer( Address thisAddress, DataStore store ) {

        def allNodes = store.findAllNodesData()
        def thisNode = allNodes.find { NodeData it -> it.address == thisAddress }
        this.cluster = new ClusterRenderer(allNodes)
        this.cluster.allJobs = TextLabel.of(store.countJobs()).number()
        this.node = new NodeRender( thisNode )

        this.workers = thisNode.workers.values().collect { WorkerData item ->
            def job = item.currentJobId ? store.getJob(item.currentJobId) : null
            new WorkerRenderer(item,job)

        }
    }

    def void compareWith( ScreenRenderer that, Closure closure ) {
        super.compareWith( that, closure )

        cluster.compareWith( that.cluster, closure )
        node.compareWith( that.node, closure )

        workers?.eachWithIndex { WorkerRenderer it, index ->
            if( index < that.workers?.size() ) {
                it.compareWith( that.workers[index], closure )
            }
        }
    }

    def void noCompare() {
        super.noCompare()
        cluster.noCompare()
        node.noCompare()
        workers.each { it.noCompare() }
    }


    StringBuilder render( StringBuilder builder = new StringBuilder() ) {

        builder <<
        """\
        Circo Cluster System Monitor
        ===========================
        """
        .stripIndent()

        cluster.render(builder)
        node.render(builder)


        builder <<
        """
        Worker           prss / err  job-id  attp
        -----------------------------------------
        """
        .stripIndent()

        workers.sort{ it.worker }.each { WorkerRenderer row ->

            row.render(builder) << '\n'
        }

        return builder
    }

}


class DataHolder {

    /** The object against with compare */
    private DataHolder shadow

    /** The closure invoked for 'diff' values */
    private Closure wrapper

    final protected Map data = [:]

    public void set( String name, def val ) {
        assert name
        data[name] = val
    }

    /**
     * Intercept getter method invoking the closure when a value is changed
     */
    public get( String name ) {

        def val = data[name]

        if ( shadow && wrapper ) {
            def other = shadow?.data?.get(name)
            if( val != other ) {
                return wrapper.call( val )
            }
        }

        return val
    }


    def void compareWith( def thatObj, Closure wrapper )  {
        this.shadow = thatObj
        this.wrapper = wrapper
    }

    def void noCompare() {
        this.shadow=null;
        this.wrapper=null
    }


}

@EqualsAndHashCode(includes='value')
class TextLabel {

    enum Align { RIGHT, LEFT }

    int pad

    Align align = Align.LEFT

    boolean active

    def value

    def List<LabelDecorator> decorators = []

    /**
     * Create a label with the provided value
     */
    TextLabel( def value ) { this.value = value }

    def TextLabel pad( int num ) { this.pad = num; this }

    def TextLabel left() { this.align = Align.LEFT; this }

    def TextLabel right() { this.align = Align.RIGHT; this }

    def TextLabel number() { this.align = Align.RIGHT; this }

    /**
     * Switch OFF the decorators rendering
     */
    def TextLabel switchOff() { active = false; this }

    /**
     * Turn ON the decorators rendering
     */
    def TextLabel switchOn() {
        if ( !decorators ) {
            decorators << AnsiStyle.style().negative()
        }

        active = true;
        return this
    }


    def TextLabel leftShift( LabelDecorator deco ) {
        this.decorators.add(deco)
        return this
    }

    def TextLabel add( LabelDecorator deco ) {
        this.decorators.add(deco)
        return this
    }

    /**
     * @return Renders the string applying the provided decorators
     */
    def String toString() {

        String result = value ? value.toString() : '-'

        if( pad && align == Align.LEFT ) {
            result = result.padRight(pad)
        }
        else if ( pad && align == Align.RIGHT ) {
            result = result.padLeft(pad)
        }
        else {
            result
        }

        if( !active )  {
            return result
        }

        decorators.each {
            result = it.apply(this,result)
        }
        return result
    }

    /**
     * Create a new {@code TextLabel} object with the specified value
     */
    static TextLabel of( def value ) {
        new TextLabel( value )
    }

}

/**
 * A generic decorator for {@code TextLabel} class
 */
interface LabelDecorator {

    String apply( TextLabel label, String value )

}

/**
 * Implements an ANSI term string decorator to be applied
 * to a {@code TextLabel} object.
 *
 * Usage:
 * <code>
 *     label << AnsiStyle.style.bold().underline()
 *     </code>
 */
class AnsiStyle implements LabelDecorator {

    Ansi.Attribute attribute

    Ansi.Color fgColor

    Ansi.Color bgColor

    def static style() { new AnsiStyle() }

    def static error() { new AnsiStyle().fg(Ansi.Color.RED).negative(); }

    def AnsiStyle bg( Ansi.Color color) { this.bgColor=color; this }

    def AnsiStyle fg( Ansi.Color color) { this.fgColor=color; this }

    def AnsiStyle attr( Ansi.Attribute attribute ) { this.attribute = attribute; this }

    def AnsiStyle bold() { attr(Ansi.Attribute.INTENSITY_BOLD); this }

    def AnsiStyle underline() { attr(Ansi.Attribute.UNDERLINE); this }

    def AnsiStyle negative() { attr(Ansi.Attribute.NEGATIVE_ON); this }


    final String apply( TextLabel label, String value ) {

        if( attribute || fgColor || bgColor ) {
            def ansi = ansi()
            if( attribute ) ansi.a(attribute)
            if ( fgColor ) ansi.fg(fgColor)
            if ( bgColor ) ansi.bg(bgColor)

            return ansi.render(value).reset()
        }

        return value
    }

}