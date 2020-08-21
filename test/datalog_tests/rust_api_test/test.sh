#!/bin/bash

set -ex

stack install

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
DATALOG_TEST_DIR="${THIS_DIR}/../"

(cd "${DATALOG_TEST_DIR}" && ddlog -i tutorial.dl -L ../../lib)

(cd "${THIS_DIR}" && cargo run)
