#!/bin/bash
# Shell script to build and run a Java program tied to ./x.dl

set -ex

source ../build_java.sh

compile x.dl XTest.java debug
java -Djava.library.path=. XTest > xtest.dump
diff xtest.dump xtest.dump.expected
cleanup
rm xtest.dump
# Additional cleanup
# rm -rf x_ddlog
