#!/bin/bash
# Shell script to build and run a Java program
# tied to test/datalog_tests/redist_opt.dl program

set -ex

# Source memory limit helper if available
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "${SCRIPT_DIR}/../../test/datalog_tests/run-with-memory-limit.sh" ]; then
    source "${SCRIPT_DIR}/../../test/datalog_tests/run-with-memory-limit.sh"
fi

source ../build_java.sh
compile ../../test/datalog_tests/redist_opt.dl Test.java release

# Run with memory limit if set
if [ -n "${MEMORY_LIMIT_GB}" ] && [ -f "${SCRIPT_DIR}/../../test/datalog_tests/run-with-memory-limit.sh" ]; then
    run_with_memory_limit java -Djava.library.path=. -cp "${CLASSPATH}" Test > test.dump
else
    java -Djava.library.path=. -cp "${CLASSPATH}" Test > test.dump
fi
diff test.dump test.dump.expected
cleanup
rm -f test.dump replay.dat
# additional cleanup
# rm -rf ../../test/datalog_tests/redist_opt_ddlog
