#!/bin/bash
# Shell script to build and run the span_uuid.dl program in Java

set -ex

source ../build_java.sh
compile ../../test/datalog_tests/span_uuid.dl SpanTest.java debug
java -Djava.library.path=. -cp ../ddlogapi.jar:. SpanTest ../../test/datalog_tests/span_uuid.dat > span_uuid.java.dump 2> span_uuid.log
diff -q span_uuid.java.dump ../../test/datalog_tests/span_uuid.dump.expected
cleanup
rm -f span_uuid.log replay.dat span_uuid.java.dump
# Additional cleanup
# rm -rf ../../test/datalog_tests/span_uuid_ddlog
