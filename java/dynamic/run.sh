#!/bin/bash
# Shell script to test the dynamic compilation and execution of a DDlog
# program starting from a Java program

set -ex

# Build the Java library with the DDlog API
make -C ..
CLASSPATH=$(pwd)/../ddlogapi.jar:..:$CLASSPATH
javac -encoding utf8 -Xlint:unchecked XTest.java
java -Djava.library.path=.  XTest > xtest.dump
diff xtest.dump xtest.dump.expected
rm xtest.dump libddlogapi.so XTest.class
# Additional cleanup
# rm -rf x_ddlog
