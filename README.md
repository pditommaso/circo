CIRCO
=====

An elastic cluster engine for the cloud.

Rationale
---------

A legacy grid engine requires many complex and time consuming system operations to be installed and managed. These activities
were considered fairly unusual operations, normally managed by the system administrator or by the IT department.

Moreover this kind of system is based on a master/slave model which is characterized by having many single points of
failure, and requiring a very high performance network connection to work properly.

In the cloud era this approach does not work any more. A cloud based system requires a more "fluid" operational environment,
in which new resources can be added or removed seamlessly and in a timely manner.

It should also be able to work with low-cost hardware or "utility" computers that, by definition,
are the building blocks of cloud based systems.

*Circo* is a distributed, decentralized grid engine that is able to automatically manage these kinds of operations.
Moreover it provides an high-level job monitoring, error detection and fault recovery mechanism, which makes it an ideal
tool for cloud deployed cluster environment.


Required dependencies
---------------------

Java 6 or higher


Build
------

The Circo build process is based on the Gradle build automation system. It can be download at the following link
http://gradle.org/


By having Gradle installed, you can compile Circo by typing the following command in the project home directory on your computer:

    $ gradle compile


You may use the following command to create the Circo distribution package and install it on the remote nodes:

    $ gradle dist


Install
-------

Launch the Circo server in each node which will make up your cluster by using the `./bin/circo-daemon.sh` script in the
project bin folder.

For testing purposes, multiple daemon instances can be started  on the same computer by specifying a different TCP port by
using the `--port` command line option.

To interact and submit jobs to the cluster use the `circo` command line tool available in the bin folder in the project directory,
and specify the IP address of one of the nodes making up the cluster. For example:

    $ ./bin/circo --host 192.168.0.1


When a daemon is running on the local machine, you are not required to specify the node IP address you want to connect to.

For development purposes the `circo` tool can be executed with an embedded server. It can be used to develop and run
your scripts locally without having to connect to a remote cluster.



License
-------

The *Circo* framework source code is released under the GNU GPL3 License.


Contact
-------
Paolo Di Tommaso - paolo (dot) ditommaso (at) gmail (dot) com