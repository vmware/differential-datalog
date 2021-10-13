#!/bin/bash

set -ex

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

(cd "${THIS_DIR}" && \
    stack install && \
    ddlog -L../../../lib -i test.dl && \
    (cd test_ddlog && cargo build) && \
    (RUST_BACKTRACE=full test_ddlog/target/debug/test_cli --self-profiler < test.dat) \
)
