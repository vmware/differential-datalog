#!/bin/sh

# compile ../copy.dl, then compile and run api_test
# NOTE: when using DDlog outside of the DDlog source tree, replace
# `stack test --ta "-p copy"` with the following commands to compile the DDlog program:
#   > ddlog -i copy.dl -L../../lib
#   > cd copy_ddlog
#   > cargo build
stack test --ta "-p copy" && cargo run
