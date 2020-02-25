#!/usr/bin/env bash

set -e

# The DDlog program to use for tests (relative to the DDlog root)
TEST_PROG=test/types_test/typesTest.dl

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

DDLOG_ROOT=${THIS_DIR}/..
TEST_DIR=${DDLOG_ROOT}/$(dirname ${TEST_PROG})
TEST_BASE=$(basename ${TEST_PROG} .dl)
TEST_NAME=${TEST_BASE}.dl
DDLOGFLAGS="--output-input-relations=O --dynlib"

echo "Running DDlog and generating .so for tests"

(cd ${TEST_DIR} && ddlog -i ${TEST_NAME} ${DDLOGFLAGS} -L ${DDLOG_ROOT}/lib -o ${THIS_DIR})

(cd ${TEST_BASE}_ddlog && cargo build --release)

export CGO_CPPFLAGS="-I${THIS_DIR}/${TEST_BASE}_ddlog"
export CGO_LDFLAGS="-L${THIS_DIR}/${TEST_BASE}_ddlog/target/release -ltypesTest_ddlog"
export LD_LIBRARY_PATH="${THIS_DIR}/${TEST_BASE}_ddlog/target/release"
