#!/bin/bash

# Test Antrea controller. 

set -e

#export DDLOGFLAGS="--output-input-relations=O --output-internal-relations"
../datalog_tests/run-test.sh networkpolicy_controller.dl release

# $1 - number of workers
# $2 - data file
# $3 - expected output file
# $4 - DIFFERENTIAL_EAGER_MERGE value
run_test() {
    echo Correctness test with $1 workers, input file \"$2\", reference file \"$3\" and DIFFERENTIAL_EAGER_MERGE=$4
    if [ $# == 4 ]; then
        export DIFFERENTIAL_EAGER_MERGE=$4
    else
        unset DIFFERENTIAL_EAGER_MERGE
    fi
    /usr/bin/time ./networkpolicy_controller_ddlog/target/release/networkpolicy_controller_cli -w $1 --no-print --no-store < $2 > antrea.dump

    sed -n '/Profile:/q;p' antrea.dump > antrea.dump.truncated
    sed -n '/Profile:/q;p' $3 > $3.truncated

    # The output should be $1 copies of redist_opt.dump.expected
    diff -q $3.truncated antrea.dump.truncated
}

run_test 1 "antrea.dat" "antrea.dump.expected"
run_test 1 "antrea.dat" "antrea.dump.expected" 100000

run_test 2 "antrea.dat" "antrea.dump.expected"
run_test 2 "antrea.dat" "antrea.dump.expected" 100000
