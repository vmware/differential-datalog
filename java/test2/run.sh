#!/bin/bash
# Shell script to build and run the a combination of two DDlog programs
# in the same process

set -ex

source ../build_java.sh
compile two.dl TwoTest.java debug
java -Djava.library.path=. -cp "${CLASSPATH}" TwoTest
cleanup
# Additional cleanup
# rm -rf two_ddlog
