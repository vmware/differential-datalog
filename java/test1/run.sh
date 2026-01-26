#!/bin/bash
# Shell script to build and run the redist.dl program in Java

set -ex

# Source memory limit helper if available
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "${SCRIPT_DIR}/../../test/datalog_tests/run-with-memory-limit.sh" ]; then
    source "${SCRIPT_DIR}/../../test/datalog_tests/run-with-memory-limit.sh"
fi

source ../build_java.sh

if [ ! -f fastutil-8.3.0.jar ]; then
    wget https://repo1.maven.org/maven2/it/unimi/dsi/fastutil/8.3.0/fastutil-8.3.0.jar
fi
CLASSPATH=$CLASSPATH:fastutil-8.3.0.jar
compile ../../test/datalog_tests/redist.dl RedistTest.java release
echo classpath: $CLASSPATH

# Run with memory limit if set (redist.dl uses SCC which triggers OOM)
if [ -n "${MEMORY_LIMIT_GB}" ] && [ -f "${SCRIPT_DIR}/../../test/datalog_tests/run-with-memory-limit.sh" ]; then
    run_with_memory_limit java -Djava.library.path=. -cp "${CLASSPATH}" RedistTest ../../test/datalog_tests/redist.dat > redist.java.dump
else
    java -Djava.library.path=. -cp "${CLASSPATH}" RedistTest ../../test/datalog_tests/redist.dat > redist.java.dump
fi
gzip -f redist.java.dump
zdiff -q redist.java.dump.gz ../../test/datalog_tests/redist.dump.expected.gz
gzip -f record.dat
zdiff -q record.dat.gz record.dat.expected.gz
cleanup
rm -rf redist.java.dump.gz fastutil-8.3.0
# Additional cleanup
# rm -rf ../../test/datalog_tests/redist_ddlog
