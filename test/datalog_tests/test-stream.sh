#!/bin/bash

set -e

./run-test.sh stream release

run_test() {
    echo Running mem leak test for $1 iterations.
    ( for (( i=1; i<=$1; i++ ))
    do
        echo "start;
        insert Chunk(\"{\\\"field\\\": $i}\"),
        insert Chunk(\"{\\\"fild\\\": $i}\"),
        commit dump_changes;"
    done) |
        /usr/bin/time -o stream_mem -f "%M" ./stream_ddlog/target/release/stream_cli -w 4 --no-store > stream.dump
}

run_memleak_test() {
    run_test 100
    mem_short=$(cat stream_mem)
    run_test 100000
    mem_long=$(cat stream_mem)
    echo "mem_short=$mem_short, mem_long=$mem_long"
    if [ "$mem_long" -gt $(( 2*"$mem_short" )) ]
    then
        echo "Possible memory leak: mem_short=$mem_short, mem_long=$mem_long"
        exit 1
    fi
}

run_memleak_test
