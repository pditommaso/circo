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

package circo.util

import groovy.transform.CompileStatic

import java.lang.reflect.Field

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@CompileStatic
class ProcessHelper {

    static int getPid(Process process) {

        assert process
        assert process.class.name == "java.lang.UNIXProcess"


        Class proc = process.getClass();
        Field field = proc.getDeclaredField("pid");
        field.setAccessible(true);
        Object pid = field.get(process);
        return (Integer) pid;

    }

}
