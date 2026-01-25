#!/bin/bash

set -ex

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

(cd "${THIS_DIR}" && \
    stack install && \
    ddlog -L../../../lib -i test.dl && \
    # Optimize for disk usage: disable incremental, reduce debuginfo
    export CARGO_INCREMENTAL=0 && \
    export RUSTFLAGS="-C debuginfo=1" && \
    (cd test_ddlog && CARGO_INCREMENTAL=0 RUSTFLAGS="-C debuginfo=1" cargo build) && \
    (RUST_BACKTRACE=full test_ddlog/target/debug/test_cli --self-profiler < test.dat) \
)
