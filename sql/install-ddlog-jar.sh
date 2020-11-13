#!/bin/bash

pushd ../java
make
mvn install:install-file -Dfile=ddlogapi.jar -DgroupId=ddlog -DartifactId=ddlogapi -Dversion=0.1 -Dpackaging=jar
popd
