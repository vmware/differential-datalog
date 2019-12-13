#!/bin/sh

cd ../java
mvn install:install-file -Dfile=ddlogapi.jar -DgroupId=ddlog -DartifactId=ddlogapi -Dversion=0.1 -Dpackaging=jar
