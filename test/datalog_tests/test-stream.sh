#!/bin/bash

set -e

# Capture script directory for use in functions
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Source memory limit helper if available
if [ -f "${SCRIPT_DIR}/run-with-memory-limit.sh" ]; then
    source "${SCRIPT_DIR}/run-with-memory-limit.sh"
fi

./run-test.sh streams release

DIFFERENTIAL_EAGER_MERGE=100

run_test() {
    echo Running mem leak test for $1 iterations.
    ( for (( i=1; i<=$1; i++ ))
    do
        echo "start;
        insert Chunk(\"{\\\"field\\\": $i}\"),
        insert Chunk(\"{\\\"fild\\\": $i}\"),
        commit dump_changes;"
    done) |
        if [ -n "${MEMORY_LIMIT_GB}" ] && [ -f "${SCRIPT_DIR}/run-with-memory-limit.sh" ]; then
            /usr/bin/time -o stream_mem -f "%M" "${SCRIPT_DIR}/run-with-memory-limit.sh" ./streams_ddlog/target/release/streams_cli -w 4 --no-store > stream_mem.dump
        else
        /usr/bin/time -o stream_mem -f "%M" ./streams_ddlog/target/release/streams_cli -w 4 --no-store > stream_mem.dump
        fi
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

run_stream_query_test() {
    echo Running streaming queries test for $1 batches of $2 queries and $3 workers;
    (
    # Populate the store.
    echo "start;"
    for (( i=1; i<=$2; i++ ))
    do
        echo "insert KVStore(${i}, \"${i}\"),"
    done
    echo "commit dump_changes;"

    # Run queries
    for (( i=1; i<=$1; i++ ))
    do
        echo "start;"
        for (( j=1; j<=$2; j++ ))
        do
            echo "insert KVStreamQuery(${j}),"
        done
        echo "commit dump_changes;"
    done) > stream_bench.dat
    if [ -n "${MEMORY_LIMIT_GB}" ] && [ -f "${SCRIPT_DIR}/run-with-memory-limit.sh" ]; then
        /usr/bin/time "${SCRIPT_DIR}/run-with-memory-limit.sh" ./streams_ddlog/target/release/streams_cli -w $3 --no-store < stream_bench.dat > stream_stream_queries.dump
    else
    /usr/bin/time ./streams_ddlog/target/release/streams_cli -w $3 --no-store < stream_bench.dat > stream_stream_queries.dump
    fi

}

run_rel_query_test() {
    echo Running relational queries test for $1 batches of $2 queries and $3 workers;
    (
    # Populate the store.
    echo "start;"
    for (( i=1; i<=$2; i++ ))
    do
        echo "insert KVStore(${i}, \"${i}\"),"
    done
    echo "commit dump_changes;"

    # Run queries
    for (( i=1; i<=$1; i++ ))
    do
        echo "start;"
        for (( j=1; j<=$2; j++ ))
        do
            echo "insert KVRelQuery(${j}),"
        done
        echo "commit dump_changes;"
        echo "start;"
        for (( j=1; j<=$2; j++ ))
        do
            echo "clear KVRelQuery;"
        done
        echo "commit dump_changes;"

    done) > stream_bench.dat
    if [ -n "${MEMORY_LIMIT_GB}" ] && [ -f "${SCRIPT_DIR}/run-with-memory-limit.sh" ]; then
        /usr/bin/time "${SCRIPT_DIR}/run-with-memory-limit.sh" ./streams_ddlog/target/release/streams_cli -w $3 --no-store < stream_bench.dat > stream_rel_queries.dump
    else
    /usr/bin/time ./streams_ddlog/target/release/streams_cli -w $3 --no-store < stream_bench.dat > stream_rel_queries.dump
    fi

}

run_count_unique_test() {
    echo Running unique user count test $1 for $2 batches of $3 events;

    (
    for (( i=1; i<=$2; i++ ))
    do
        echo "echo Transaction $i;"
        echo "start;"
        for (( j=1; j<=$3; j++ ))
        do
            echo "insert UserSession$1($(($i * $2 + $j))),"
            #echo "insert UserSession$1($j),"
        done
        echo "commit dump_changes;"
    done
    ) > unique_bench$1.dat
    if [ -n "${MEMORY_LIMIT_GB}" ] && [ -f "${SCRIPT_DIR}/run-with-memory-limit.sh" ]; then
        /usr/bin/time "${SCRIPT_DIR}/run-with-memory-limit.sh" ./streams_ddlog/target/release/streams_cli -w 1 --no-store < unique_bench$1.dat > unique_bench$1.dump
    else
    /usr/bin/time ./streams_ddlog/target/release/streams_cli -w 1 --no-store < unique_bench$1.dat > unique_bench$1.dump
    fi
    diff -q unique_bench$1.dump unique_bench$1.dump.expected
}

run_count_unique_test 1 1000 100
run_count_unique_test 2 1000 100

run_stream_query_test 1000 1000 1
run_stream_query_test 1000 1000 2
run_stream_query_test 1000 1000 4
run_rel_query_test 1000 1000 1
run_rel_query_test 1000 1000 2
run_rel_query_test 1000 1000 4

run_memleak_test
