import rush.JobExecutorTest
import rush.JobMasterTest
import rush.client.cmd.CmdStatTest
import rush.client.cmd.CommandParserTest
import rush.data.NodeDataTest
import rush.data.WorkerDataTest
import rush.frontend.AbstractCmdResultTest
import rush.frontend.FrontEndTest
import rush.messages.JobEntryTest
import rush.messages.JobIdTest
import rush.messages.JobStatusTest
import rush.ui.*
import org.junit.runner.RunWith
import org.junit.runners.Suite
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

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */

@RunWith(Suite)
@Suite.SuiteClasses([
    JobIdTest,
    JobEntryTest,
    FrontEndTest,
    AbstractCmdResultTest,
    WorkerDataTest,
    NodeDataTest,
    JobMasterTest,
    JobExecutorTest,
    TerminalUITest,
    TextLabelTest,
    ClusterRendererTest,
    DataHolderTest,
    ScreenRendererTest,
    CmdStatTest,
    JobStatusTest,
    CommandParserTest

    ])
class AllTests {
}
