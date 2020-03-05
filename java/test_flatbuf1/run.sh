#!/bin/bash
# Compile and run tests related to typesTest.dl

set -ex

source ../build_java.sh
DDLFLAGS="--output-input-relations=O"
compile ../../test/types_test/typesTest.dl Test.java release
java -Djava.library.path=. Test
diff rec_dynamic.dump rec_dynamic.dump.expected
diff fb.dump rec.dump
diff fb.dump fb.dump.expected
diff query.dump query.dump.expected
cleanup
rm fb.dump rec.dump
# Additional cleanup
# rm -rf typesTest_ddlog
