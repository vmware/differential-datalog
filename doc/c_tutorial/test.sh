#!/bin/bash

# Run C API tutorial example.

set -ex

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# When running in CI, the DDlog compiler should be preinstalled by the build stage.
if [ -z "${IS_CI_RUN}" ]; then
    stack install
fi

ddlog -i "${THIS_DIR}/t1_reachability_monitor.dl"
(cd "${THIS_DIR}/t1_reachability_monitor_ddlog/" && cargo build --release)
(cd "${THIS_DIR}" && \
    gcc t1_reachability_monitor.c t1_reachability_monitor_ddlog/target/release/libt1_reachability_monitor_ddlog.a -It1_reachability_monitor_ddlog/ -lpthread -ldl -lm && \
    ./a.out <<'EOF'
Menlo Park
Santa Barbara
true
EOF
)
