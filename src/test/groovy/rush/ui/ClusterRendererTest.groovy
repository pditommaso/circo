/*
 * Copyright (c) 2012, the authors.
 *
 *    This file is part of Rush.
 *
 *    Rush is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation, either version 3 of the License, or
 *    (at your option) any later version.
 *
 *    Rush is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with Rush.  If not, see <http://www.gnu.org/licenses/>.
 */

package rush.ui

import spock.lang.Specification

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class ClusterRendererTest extends Specification {


    def testSimple () {

        when:
        def cluster = new ClusterRenderer()
        cluster.allJobs = 1
        cluster.processed = 2
        cluster.failed = 3

        then:
        cluster.allJobs == 1
        cluster.processed == 2
        cluster.failed == 3
    }


    def "test compare" () {

        when:
        def c1 = new ClusterRenderer()
        c1.processedJobs = 100
        c1.failedJobs = 3

        def c2 = new ClusterRenderer()
        c2.processedJobs = 101
        c2.failedJobs = 3


        c2.compareWith(c1) { value ->
            "* ${value} *"
        }

        then:

        c2.failedJobs == 3
        c2.processedJobs == "* 101 *"
    }



    def "test NoCompare " () {

        setup:
        def c1 = new ClusterRenderer( processedJobs: 100, failedJobs: 3 )
        def c2 = new ClusterRenderer( processedJobs: 101, failedJobs: 3 )
        c2.compareWith(c1) { value, oldValue -> "* ${value} *" }

        when:
        c2.noCompare()

        then:
        c1.processedJobs == 100
        c1.failedJobs == 3

        c2.processedJobs == 101
        c1.failedJobs == 3

    }
}
