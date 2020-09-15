#!/bin/bash

set -ex

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
TEST_DIR="${THIS_DIR}/server_api"

(cd "${THIS_DIR}" && ddlog -i server_api.dl --omit-profile --omit-workspace)

(
    cd "${TEST_DIR}" &&
    i=0 &&
    true &&
    #while [ $? -eq 0 -a $i -lt 100 ]; do
    while [ $? -eq 0 -a $i -lt 10 ]; do
        i=$((i+1));
        #CARGO_LOG=cargo::core::compiler::fingerprint=trace RUST_LOG=trace cargo test
        RUST_LOG=trace cargo test
    done
)
