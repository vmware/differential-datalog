#!/bin/bash

set -e

# Clone or refresh the repo.
DCM_DATA_REPO=https://github.com/ddlog-dev/dcm-test-data.git
LOCAL_DCM_DATA_REPO=dcm-test-data

if [ ! -d "${LOCAL_DCM_DATA_REPO}/.git" ]
then
    git clone $DCM_DATA_REPO
else
    (cd $LOCAL_DCM_DATA_REPO && git pull $DCM_DATA_REPO)
fi

./run-test.sh dcm1 release

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
    /usr/bin/time ./dcm1_ddlog/target/release/dcm1_cli -w $1 --no-print --no-store < $2 > dcm1.dump

    diff -q $3 dcm1.dump
}

run_test 1 "dcm-test-data/dcm_large.dat" "dcm-test-data/dcm_large.dump.expected"
run_test 1 "dcm-test-data/dcm_large.dat" "dcm-test-data/dcm_large.dump.expected" 100
run_test 1 "dcm-test-data/dcm_large.dat" "dcm-test-data/dcm_large.dump.expected" 100000

run_test 2 "dcm-test-data/dcm_large.dat" "dcm-test-data/dcm_large.dump.expected"
run_test 2 "dcm-test-data/dcm_large.dat" "dcm-test-data/dcm_large.dump.expected" 100
run_test 2 "dcm-test-data/dcm_large.dat" "dcm-test-data/dcm_large.dump.expected" 100000

run_test 4 "dcm-test-data/dcm_large.dat" "dcm-test-data/dcm_large.dump.expected"
run_test 4 "dcm-test-data/dcm_large.dat" "dcm-test-data/dcm_large.dump.expected" 100
run_test 4 "dcm-test-data/dcm_large.dat" "dcm-test-data/dcm_large.dump.expected" 100000
