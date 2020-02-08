#!/bin/bash

# Test Antrea controller. 

set -e

export DDLOGFLAGS="--output-input-relations=O --output-internal-relations"
../datalog_tests/run-test.sh networkpolicy_controller.dl release
