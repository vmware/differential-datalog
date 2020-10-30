#!/bin/bash

set -ex

# When running in CI, the DDlog compiler should be prinstalled by the build stage.
if [ -z "${IS_CI_RUN}" ]; then
    stack install
fi

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
DATALOG_TEST_DIR="${THIS_DIR}/../"

(cd "${DATALOG_TEST_DIR}" && ddlog -i tutorial.dl -L ../../lib)

(cd "${THIS_DIR}" && cargo run)
