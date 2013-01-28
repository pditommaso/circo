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

package circo

import akka.actor.AddressFromURIString
import circo.data.FileRef
import circo.messages.JobContext
import circo.messages.JobEntry
import circo.messages.JobId
import circo.messages.JobReq
import circo.messages.JobResult
import spock.lang.Specification

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class JobExecutorTest extends Specification {


    def "test createRushDir" () {

        when:
        def entry = new JobEntry(JobId.of(123), new JobReq(script: 'echo Hello world!'))
        entry.workDir = new File(System.getProperty('java.io.tmpdir'))
        def file = JobExecutor.createPrivateDir(entry)

        then:
        file.exists()
        file.isDirectory()
        file.name == '.circo'

        cleanup:
        file.delete()

    }


    def "test createWorkDir" () {

        when:
        def path = JobExecutor.createScratchDir(123554)
        println path

        then:
        path.exists()
        path.isDirectory()

        cleanup:
        path.delete()
    }

    def 'test stage' () {

        setup:
        def req = new JobReq()
        req.script = '''
        cp $file1 file2
        run file1_txt
        run file2_txt
        '''
        .stripIndent().trim()
        req.get = ['file1']
        req.produce = ['file2']
        req.context = new JobContext().put(new FileRef('/path/on/the/fs/file1'))

        when:
        def script = JobExecutor.stage(req)
        def lines = script.readLines()

        then:
        lines.size() == 3
        lines[0] == "cp /path/on/the/fs/file1 file2"
        lines[1] == "run file1_txt"
        lines[2] == "run file2_txt"

    }


    def 'test gather' () {

        setup:
        def script = """
        cp file1 file2
        run file1_txt
        run file2_txt
        echo hello > file3
        """

        def req = new JobReq(script:script)
        req.context = new JobContext().put(new FileRef('/path/on/the/fs/file1'))
        def filesToProduce = ['file2.txt','file3.txt']

        def workDir = new File(System.properties['java.io.tmpdir'] as String).absoluteFile
        def files = filesToProduce.collect { new File(workDir,it) }
        files.each { it.createNewFile() }

        def result = new JobResult()
        def addr = AddressFromURIString.parse('akka://sys@1.1.1.1:2555')

        when:
        req.produce = filesToProduce
        def ctx = JobExecutor.gather( req, result, workDir, addr).context

        then:
        !ctx.contains('file1.txt')
        ctx.contains('file2.txt')
        ctx.contains('file3.txt')

        (ctx.getRef('file2.txt')[0] as FileRef).name == 'file2.txt'
        (ctx.getRef('file2.txt')[0] as FileRef).address == addr
        (ctx.getRef('file2.txt')[0] as FileRef).data == files[0]

        (ctx.getRef('file3.txt')[0] as FileRef).name == 'file3.txt'
        (ctx.getRef('file3.txt')[0] as FileRef).address == addr
        (ctx.getRef('file3.txt')[0] as FileRef).data == files[1]

        cleanup:
        files.each { it.delete() }

    }

    def 'test gather with pattern' () {

        setup:
        def script = """
        cp file1 file2
        run file1_txt
        run file2_txt
        echo hello > file3
        """

        def req = new JobReq(script:script)
        req.context = new JobContext().put(new FileRef('/path/on/the/fs/file1'))
        def filesToProduce = ['file1.txt', 'file2.txt','file3.txt', 'fasta.fa']

        def workDir = new File(System.properties['java.io.tmpdir'] as String).absoluteFile
        def files = filesToProduce.collect { new File(workDir,it) }
        files.each { it.createNewFile() }

        def result = new JobResult()
        def addr = AddressFromURIString.parse('akka://sys@1.1.1.1:2555')

        when:
        req.produce = ['file*','fasta.fa']
        def ctx = JobExecutor.gather( req, result, workDir, addr).context

        then:
        ctx.contains('file1.txt')
        ctx.contains('file2.txt')
        ctx.contains('file3.txt')
        ctx.contains('fasta.fa')
        !ctx.contains('file5.txt')

        (ctx.getRef('file1.txt')[0] as FileRef).name == 'file1.txt'
        (ctx.getRef('file1.txt')[0] as FileRef).address == addr
        (ctx.getRef('file1.txt')[0] as FileRef).data == files[0]

        (ctx.getRef('file2.txt')[0] as FileRef).name == 'file2.txt'
        (ctx.getRef('file2.txt')[0] as FileRef).address == addr
        (ctx.getRef('file2.txt')[0] as FileRef).data == files[1]

        cleanup:
        files.each { it.delete() }

    }


    def 'test gather with variable aggregation' () {

        setup:
        def script = """
        cp file1 file2
        run file1_txt
        run file2_txt
        echo hello > file3
        """

        def req = new JobReq(script:script)
        req.context = new JobContext().put(new FileRef('/path/on/the/fs/file1'))
        def filesToProduce = ['file1.txt', 'file2.txt','file3.txt', 'aln.fa']

        def workDir = new File(System.properties['java.io.tmpdir'] as String).absoluteFile
        def files = filesToProduce.collect { new File(workDir,it) }
        files.each { it.createNewFile() }

        def result = new JobResult()
        def addr = AddressFromURIString.parse('akka://sys@1.1.1.1:2555')

        when:
        req.produce = ['step_result:file*','fasta_result:aln.fa']
        def ctx = JobExecutor.gather( req, result, workDir, addr).context

        then:
        ctx.contains('step_result')
        ctx.contains('fasta_result')
        !ctx.contains('file3.txt')

        ctx.getRef('step_result').size() == 3
        ctx.getRef('fasta_result').size() == 1


        cleanup:
        files.each { it.delete() }

    }



}
