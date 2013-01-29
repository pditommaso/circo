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



import org.hyperic.sigar.Sigar
import spock.lang.Specification
import test.TestHelper

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class SigarTest extends Specification {

    def "test sigar" () {
        setup:
        TestHelper.updateJavaLibPath()

        when:
        def sigar = new Sigar()
        println "Mem actual free: " +  sigar.getMem()
        println "CPU: " + sigar.getCpuPerc()
        println "Uptime: " + sigar.getUptime()
        println "Net stat: " + sigar.getNetStat()
        println "Net info: " + sigar.getNetInfo()
        println "FS List" + sigar.getFileSystemList()
        sigar.getFileSystemList().each {
            println "Usage stat for $it: " + sigar.getFileSystemUsage( it.toString() )
        }


        then:
        mem.getTotal() > 0

    }
}
