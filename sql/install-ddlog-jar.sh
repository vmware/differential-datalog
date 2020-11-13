#!/bin/bash

pushd ../java
make
mvn install:install-file -Dfile=ddlogapi.jar -DgroupId=ddlog -DartifactId=ddlogapi -Dversion=0.1 -Dpackaging=jar
popd
## Generate flatbuf API for sqlRecord
#cd lib
#ddlog -i sqlRecord.dl -j
#mkdir -p sqlRecord_ddlog/flatbuf/src/main
#mv sqlRecord_ddlog/flatbuf/java sqlRecord_ddlog/flatbuf/src/main
#cp sqlRecord-pom.xml sqlRecord_ddlog/flatbuf/pom.xml
#pushd sqlRecord_ddlog/flatbuf || exit 1
#mvn package
#mvn install:install-file -Dfile=target/sqlRecord-1.0.jar -DgroupId=ddlog -DartifactId=sqlRecord -Dversion=1.0 -Dpackaging=jar
#popd
#
