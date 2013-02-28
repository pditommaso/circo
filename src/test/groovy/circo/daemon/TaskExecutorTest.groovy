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

package circo.daemon
import circo.data.LocalDataStore
import circo.model.Context
import circo.model.FileRef
import circo.model.TaskEntry
import circo.model.TaskId
import circo.model.TaskReq
import circo.model.TaskResult
import spock.lang.Specification
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class TaskExecutorTest extends Specification {


    def "test createRushDir" () {

        when:
        def entry = new TaskEntry(TaskId.of(123), new TaskReq(script: 'echo Hello world!'))
        entry.workDir = new File(System.getProperty('java.io.tmpdir'))
        def file = TaskExecutor.createPrivateDir(entry)

        then:
        file.exists()
        file.isDirectory()
        file.name == '.circo'

        cleanup:
        file.delete()

    }


    def 'test stage' () {

        setup:
        FileRef.dataStore = new LocalDataStore()

        def req = new TaskReq()
        req.script = '''
        cp $file1 file2
        run file1_txt
        run $not_a_context_var
        '''
        .stripIndent().trim()
        req.produce = ['file2']
        req.context = new Context().put(new FileRef('/path/on/the/fs/file1'))

        when:
        def script = TaskExecutor.stage(req)
        def lines = script.readLines()

        then:
        lines.size() == 3
        lines[0] == "cp /path/on/the/fs/file1 file2"
        lines[1] == "run file1_txt"
        lines[2] == 'run $not_a_context_var'

    }


    def 'test gather' () {

        setup:
        FileRef.dataStore = new LocalDataStore()

        def script = """
        cp file1 file2
        run file1_txt
        run file2_txt
        echo hello > file3
        """

        def req = new TaskReq(script:script)
        req.context = new Context().put(new FileRef('/path/on/the/fs/file1'))
        def filesToProduce = ['file2.txt','file3.txt']

        def workDir = new File(System.properties['java.io.tmpdir'] as String).absoluteFile
        def files = filesToProduce.collect { new File(workDir,it) }
        files.each { it.createNewFile() }

        def result = new TaskResult()

        when:
        req.produce = filesToProduce
        def ctx = TaskExecutor.gather( req, result, workDir ).context

        then:
        !ctx.contains('file1.txt')
        ctx.contains('file2.txt')
        ctx.contains('file3.txt')

        (ctx.getData('file2.txt') as File) == files[0]
        (ctx.getData('file2.txt') as File).name == 'file2.txt'

        (ctx.getData('file3.txt') as File) == files[1]
        (ctx.getData('file3.txt') as File).name == 'file3.txt'

        cleanup:
        files.each { it.delete() }

    }

    def 'test gather with pattern' () {

        setup:
        FileRef.dataStore = new LocalDataStore()

        def script = """
        cp file1 file2
        run file1_txt
        run file2_txt
        echo hello > file3
        """

        def req = new TaskReq(script:script)
        req.context = new Context().put(new FileRef('/path/on/the/fs/file1'))
        def filesToProduce = ['file1.txt', 'file2.txt','file3.txt', 'fasta.fa']

        def workDir = new File(System.properties['java.io.tmpdir'] as String).absoluteFile
        def files = filesToProduce.collect { new File(workDir,it) }
        files.each { it.createNewFile() }

        def result = new TaskResult()

        when:
        req.produce = ['file*','fasta.fa']
        def ctx = TaskExecutor.gather( req, result, workDir).context

        then:
        ctx.contains('file1.txt')
        ctx.contains('file2.txt')
        ctx.contains('file3.txt')
        ctx.contains('fasta.fa')
        !ctx.contains('file5.txt')

        (ctx.getData('file1.txt') as File) == files[0]
        (ctx.getData('file1.txt') as File).name == 'file1.txt'

        (ctx.getData('file2.txt') as File) == files[1]
        (ctx.getData('file2.txt') as File).name == 'file2.txt'

        cleanup:
        files.each { it.delete() }

    }


    def 'test gather with variable aggregation' () {

        setup:
        FileRef.dataStore = new LocalDataStore()

        def script = """
        cp file1 file2
        run file1_txt
        run file2_txt
        echo hello > file3
        """

        def req = new TaskReq(script:script)
        req.context = new Context().put(new FileRef('/path/on/the/fs/file1'))
        def filesToProduce = ['file1.txt', 'file2.txt','file3.txt', 'aln.fa']

        def workDir = new File(System.properties['java.io.tmpdir'] as String).absoluteFile
        def files = filesToProduce.collect { new File(workDir,it) }
        files.each { it.createNewFile() }

        def result = new TaskResult()

        when:
        req.produce = ['step_result=file*','fasta_result=aln.fa']
        def ctx = TaskExecutor.gather( req, result, workDir ).context

        then:
        ctx.contains('step_result')
        ctx.contains('fasta_result')
        !ctx.contains('file3.txt')

        (ctx.getData('step_result') as Collection).collect { File it -> it.name }.toSet() ==  ['file1.txt', 'file2.txt','file3.txt'] as Set
        (ctx.getData('fasta_result') as File) .name == 'aln.fa'


        cleanup:
        files.each { it.delete() }

    }



}
