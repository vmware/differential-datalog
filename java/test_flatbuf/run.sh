#!/bin/bash
# Shell script to build and run a Java program
# tied to test/datalog_tests/redist_opt.dl program

set -ex

source ../build_java.sh
compile ../../test/datalog_tests/redist_opt.dl Test.java release
java -Djava.library.path=. Test > test.dump
diff test.dump test.dump.expected
cleanup
rm -f test.dump replay.dat
# additional cleanup
# rm -rf ../../test/datalog_tests/redist_opt_ddlog
