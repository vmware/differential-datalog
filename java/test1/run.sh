#!/bin/bash
# Shell script to build and run the redist.dl program in Java

set -ex

source ../build_java.sh

if [ ! -f fastutil-8.3.0.jar ]; then
    wget https://repo1.maven.org/maven2/it/unimi/dsi/fastutil/8.3.0/fastutil-8.3.0.jar
fi
CLASSPATH=$CLASSPATH:fastutil-8.3.0.jar
compile ../../test/datalog_tests/redist.dl RedistTest.java release
echo classpath: $CLASSPATH
java -Djava.library.path=. RedistTest ../../test/datalog_tests/redist.dat > redist.java.dump
gzip -f redist.java.dump
zdiff -q redist.java.dump.gz ../../test/datalog_tests/redist.dump.expected.gz
cleanup
rm -rf redist.java.dump.gz fastutil-8.3.0
# Additional cleanup
# rm -rf ../../test/datalog_tests/redist_ddlog
