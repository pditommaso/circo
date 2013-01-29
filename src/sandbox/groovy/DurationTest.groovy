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



import scala.concurrent.duration.Duration

/**
 *
 *  @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */


assert Duration.create('5 seconds').toSeconds() == 5
assert Duration.create('5 second').toSeconds() == 5
assert Duration.create('5 sec').toSeconds() == 5
assert Duration.create('5 s').toSeconds() == 5
assert Duration.create('5s').toSeconds() == 5

assert Duration.create('5 minutes').toSeconds() == 300
assert Duration.create('5 minute').toSeconds() == 300
assert Duration.create('5 min').toSeconds() == 300
assert Duration.create('5min').toSeconds() == 300

assert Duration.create('5 hours').toMinutes() == 300
assert Duration.create('5 hour').toMinutes() == 300
assert Duration.create('5 h').toMinutes() == 300
assert Duration.create('5h').toMinutes() == 300

assert Duration.create('1 days').toHours() == 24
assert Duration.create('1 day').toHours() == 24
assert Duration.create('1 d').toHours() == 24
assert Duration.create('1d').toHours() == 24
assert Duration.create('1days').toHours() == 24
assert Duration.create('1day').toHours() == 24
assert Duration.create('1d').toHours() == 24