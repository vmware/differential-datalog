#!/bin/bash
# Shell script to build and run a Java program
# tied to ../../test/datalog_tests/span_string.dl program

set -ex

source ../build_java.sh

compile ../../test/datalog_tests/span_string.dl SpanTest.java release
java -Djava.library.path=. SpanTest > spantest.dump
diff spantest.dump spantest.dump.expected
cleanup
rm -f spantest.dump
# Additional cleanup
# rm -rf ../../test/datalog_tests/span_string_ddlog
