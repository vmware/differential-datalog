#!/bin/bash

# Test Antrea controller.

set -e

# When running in CI, the DDlog compiler should be preinstalled by the build stage.
if [ -z "${IS_CI_RUN}" ]; then
    if [ "x${PROFILE}" == "x1" ]; then
        stack install --profile
        export GHCRTS="-xc"
    else
        stack install
    fi
fi

# Clone or refresh the repo.
DATA_REPO_NAME="antrea-test-data"
DATA_REPO=https://github.com/ddlog-dev/${DATA_REPO_NAME}.git
DATA_REPO_BRANCH=v3

if [ ! -d "${DATA_REPO_NAME}/.git" ]
then
    git clone -b ${DATA_REPO_BRANCH} ${DATA_REPO}
else
    (cd "${DATA_REPO_NAME}" && git fetch "${DATA_REPO}" && git checkout "${DATA_REPO_BRANCH}")
fi

#export DDLOGFLAGS="--output-input-relations=O --output-internal-relations"
#../datalog_tests/run-test.sh networkpolicy_controller.dl release
ddlog -i networkpolicy_controller.dl -j -L../../lib
(cd networkpolicy_controller_ddlog && cargo build --release)

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

    if [[ $2 == *.gz ]]
    then
        gunzip -kf $2
        dat=${2%.gz}
    else
        dat=$2
    fi

    /usr/bin/time ./networkpolicy_controller_ddlog/target/release/networkpolicy_controller_cli -w $1 --no-print --no-store < $dat > antrea.dump

    # Dump profile on the terminal.
    #sed -n '/^Profile:$/,$p' antrea.dump

    # Remove profiling data, which changes across runs.
    sed -n '/Profile:/q;p' antrea.dump > antrea.dump.truncated
    if [[ $3 == *.gz ]]
    then
        gunzip -kf $3
        expected=${3%.gz}
    else
        expected=$3
    fi
    sed -n '/Profile:/q;p' $expected > $expected.truncated

    diff -q $expected.truncated antrea.dump.truncated
}

run_test 1 "antrea.dat" "antrea.dump.expected"
run_test 1 "antrea.dat" "antrea.dump.expected" 100000

run_test 2 "antrea.dat" "antrea.dump.expected"
run_test 2 "antrea.dat" "antrea.dump.expected" 100000

run_test 1 "antrea-test-data/antrea.dat.gz" "antrea-test-data/antrea.dump.expected.4.gz"
run_test 1 "antrea-test-data/antrea.dat.gz" "antrea-test-data/antrea.dump.expected.4.gz" 10
