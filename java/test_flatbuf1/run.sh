#!/bin/bash
# Compile and run tests related to flatbufTest.dl

set -ex

source ../build_java.sh
compile flatbufTest.dl Test.java release
java -Djava.library.path=. Test
diff fb.dump rec.dump
diff fb.dump fb.dump.expected
cleanup
# rm -rf fb.dump rec.dump flatbufTest_ddlog replay.dat
