#!/bin/sh

# compile ../api.dl, then compile and run api_test
# NOTE: when using DDlog outside of the DDlog source tree, replace
# `stack test --ta "-p api"` with the following commands to compile the DDlog program:
#   > ddlog -i api.dl -L../../lib
#   > cd api_ddlog
#   > cargo build --release
stack test --ta "-p api" && cargo run
