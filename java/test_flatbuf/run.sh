#!/bin/bash
# Shell script to build and run a Java program
# tied to test/datalog_tests/redist.dl program

set -ex

source ../build_java.sh
compile ../../test/datalog_tests/redist.dl Test.java debug
java -Djava.library.path=. Test > test.dump
diff test.dump test.dump.expected
cleanup
# rm -rf ../../test/datalog_tests/redist_ddlog test.dump replay.dat
