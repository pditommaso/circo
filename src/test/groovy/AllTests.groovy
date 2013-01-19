import circo.JobExecutorTest
import circo.JobMasterTest
import circo.client.cmd.CmdNodeTest
import circo.client.cmd.CmdStatTest
import circo.client.cmd.CmdSubTest
import circo.client.cmd.CommandParserTest
import circo.data.DataStoreTest
import circo.data.JdbcDataSourceFactoryTest
import circo.data.JdbcJobsMapStoreTest
import circo.data.NodeDataTest
import circo.data.WorkerDataTest
import circo.frontend.AbstractCmdResultTest
import circo.frontend.FrontEndTest
import circo.messages.JobEntryTest
import circo.messages.JobIdTest
import circo.messages.JobStatusTest
import circo.ui.*
import org.junit.runner.RunWith
import org.junit.runners.Suite
import circo.utils.RushHelperTest

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
    JobStatusTest,
    DataStoreTest,
    JdbcDataSourceFactoryTest,
    JdbcJobsMapStoreTest,
    CmdSubTest,
    CmdNodeTest,
    CmdStatTest,
    CommandParserTest,
    RushHelperTest

    ])
class AllTests {
}
