#!/bin/bash
# Shell script to build and run the redist.dl program in Java
# This test is tied to the redist.dl program.

set -ex

source ../build_java.sh

if [ ! -f fastutil-8.3.0.jar ]; then
    wget https://repo1.maven.org/maven2/it/unimi/dsi/fastutil/8.3.0/fastutil-8.3.0.jar
fi
CLASSPATH=$CLASSPATH:fastutil-8.3.0.jar:..:../ddlogapi.jar:.
compile ../../test/datalog_tests/redist.dl RedistTest.java release
java -Djava.library.path=. RedistTest ../../test/datalog_tests/redist.dat > redist.java.dump
gzip redist.java.dump
zdiff -q redist.java.dump.gz ../../test/datalog_tests/redist.dump.expected.gz

cleanup
# Additional cleanup
# rf -f redist.java.dump.gz fastutil-8.3.0 ../../test/datalog_tests/redist_ddlog
