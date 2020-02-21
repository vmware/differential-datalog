#!/usr/bin/env bash

set -e

source build-ddlog-prog.sh

# Building Go package
make

# Running Go unit tests (with benchmarks)
make check-bench

# Running Go linters
make golangci
