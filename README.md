## MckoiDDB: Mckoi Distributed Database

MckoiDDB is a distributed database system written in Java. MckoiDDB can be
installed on multiple servers and coordinates the storage, management and
access of structured data over the network. High scalability is achieved by
providing methods that allow developers to configure data sets for either
high partitionality or high consistency. The software provides a simple and
efficient data refactoring process for rebalancing a data set between
partitionality or consistency.

### License

MckoiDDB is released under the [Apache License version 2](./LICENSE.txt).

### How to Build

MckoiDDB has no external dependencies. Feel free to import the source code
into your favourite build tool or editor.

The source code is arranged in a standard Maven format and a Maven pom.xml
build script is included if you wish to use that. The following Maven
command builds the standard binary and source distributions;

    mvn assembly:single
