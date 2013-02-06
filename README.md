CIRCO
=====

Elastic cluster engine for the cloud.

Rationale
---------

Legacy grid engines require long and complex system operations to be installed or simply to add/remove new nodes to
the computing environment. These requirements were considered extraordinary maintenance operations to be managed
by the grid administrator and the IT people.

Moreover this kind of systems are based on a master/slave model which is characterized by having many single points of
failure, and requiring a very high performance network connection to work properly.

A cloud based system requires a more "fluid" operational environment, in which new resources can be added or removed
seamlessly and in a timely manner. It also should be able to work with low-cost hardware or "utility" computers that,
by definition, are the building blocks of cloud based systems.

Circo is a distributed, decentralized grid engine that is able to manage automatically these kind of operations.
Moreover it provides an high-level jobs monitoring, error detection and recovery mechanism, which makes it an ideal
tool for cloud deployed cluster environment.


Required dependencies
---------------------

Java 6 or higher


Compile
-------

Circo build process is based on the Gradle build automation system. It can be download at the following link
http://gradle.org/


Having Gradle installed on your computer, to compile the project type the following command
in the Circo project home directory on your computer:

    $ gradle compile


You may use the following command to create the distribution package and install it on a remote node:

    $ gradle dist


Install
-------

Launch the Circo server in each node which made up your cluster by using the `./bin/circo-daemon.sh` script in the bin project
folder.

For testing purpose multiple daemon instances can be started  on the same computer by specifying a different TCP port by
using the `--port` command line option.

To interact and submit jobs to the cluster use the `circo` command line tool available in bin folder in the project directory,
and specifying the ip address of one of the node making uo the cluster. For example:

    $ ./bin/circo --host 192.168.0.1


If a daemon is running on the local machine it is not required to specify the cluster node to which connect.

For development purpose the `circo` tool can be executed with an embedded server. It can be used to develop and run locally
your scripts without having to connect to a remote cluster.



License
-------

The *Circo* framework source code is released under the GNU GPL3 License.


Contact
-------
Paolo Di Tommaso - paolo (dot) ditommaso (at) gmail (dot) com