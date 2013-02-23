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

package circo.reply
import circo.model.Job
import circo.model.TaskEntry
import circo.util.SerializeId
import groovy.transform.InheritConstructors
import groovy.transform.ToString

/**
 * Holds the reply data for the {@code CmdList} command
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */

@ToString(includeNames = true, includePackage = false)
@InheritConstructors
class ListReply extends AbstractReply {

    List<JobInfo> jobs


    @SerializeId
    static class JobInfo implements Serializable {

        JobInfo( Job job ) {
            this.target = job
        }

        @Delegate
        Job target

        String command

        List<TaskEntry> failedTasks

        List<TaskEntry> pendingTasks


    }

}
