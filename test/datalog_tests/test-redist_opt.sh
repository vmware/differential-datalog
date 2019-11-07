#!/bin/bash

set -e

./run-test.sh redist_opt release

# $1 - number of iterations
# $2 - number of workers
# $3 - DIFFERENTIAL_EAGER_MERGE value
run_tiny_test() {
    if [ $# == 3 ]; then
        export DIFFERENTIAL_EAGER_MERGE=$3
    else
        unset DIFFERENTIAL_EAGER_MERGE
    fi
    # Feed $1 copies of data to DDlog
    ( for (( i=1; i<=$1; i++ ))
    do
        echo "start;
        insert DdlogNode[DdlogNode{0,EntityOther{}}],
        insert DdlogNode[DdlogNode{1,EntityOther{}}],
        commit;
        start;
        delete DdlogNode[DdlogNode{0,EntityOther{}}],
        delete DdlogNode[DdlogNode{1,EntityOther{}}],
        commit;"
    done) |
        /usr/bin/time -o redist_mem -f "%M" ./redist_opt_ddlog/target/release/redist_opt_cli -w $2 --no-print --no-store --no-delta
}

# $1 - DIFFERENTIAL_EAGER_MERGE value
run_memleak_test() {
    echo Mem leak test with DIFFERENTIAL_EAGER_MERGE=$1
    run_tiny_test 10 4 $1
    mem_short=$(cat redist_mem)
    run_tiny_test 10000 4 $1
    mem_long=$(cat redist_mem)
    echo "mem_short=$mem_short, mem_long=$mem_long"
    if [ "$mem_long" -gt $(( 2*"$mem_short" )) ]
    then
        echo "Possible memory leak: mem_short=$mem_short, mem_long=$mem_long"
        exit 1
    fi
}

run_memleak_test
run_memleak_test 100000

# $1 - number of iterations
# $2 - number of workers
# $3 - DIFFERENTIAL_EAGER_MERGE value
run_test() {
    echo Correctness test with $1 iterations, $2 workers, and DIFFERENTIAL_EAGER_MERGE=$3
    if [ $# == 3 ]; then
        export DIFFERENTIAL_EAGER_MERGE=$3
    else
        unset DIFFERENTIAL_EAGER_MERGE
    fi
    # Feed $1 copies of data to DDlog
    ( for (( i=1; i<=$1; i++ ))
    do
        cat redist_opt-test-data/redist_opt.dat
    done) |
        /usr/bin/time ./redist_opt_ddlog/target/release/redist_opt_cli -w $2 --no-print --no-store > redist_opt.dump

    # The output should be $1 copies of redist_opt.dump.expected
    (for (( i=1; i<=$1; i++ ))
    do
        cat redist_opt-test-data/redist_opt.dump.expected
    done) > redist_opt.dump.expected

    diff -q redist_opt.dump.expected redist_opt.dump
}

run_test 5 1
run_test 5 1 100000
run_test 3 2
run_test 3 2 100000
run_test 3 4
run_test 3 4 100000
run_test 3 8
run_test 3 8 100000
run_test 3 16
run_test 3 16 100000
run_test 3 40
run_test 3 40 100000
