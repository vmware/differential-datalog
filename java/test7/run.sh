#!/bin/bash

set -ex

source ../build_java.sh
LIBDDLOG="libmylib.so" # use a custom library name
compile a.dl A.java debug
java -Djava.library.path=. A > atest.dump
diff atest.dump atest.dump.expected
cleanup
rm libmylib.so atest.dump
# Additional cleanup
# rm -rf a_ddlog
