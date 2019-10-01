#!/bin/bash
# Runs all the tests in the current directory
# If some arguments are given they are interpreted as tests to  run

# Stop at first failure
set -e

# Extra compiler flags for some tests, indexed by test basename
declare -A extraflags
extraflags[three]="--output-internal-relations"

# Detect directory where script resides
mydir=$(dirname "$0")
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
# Always run in the directory where the script is installed
cd ${mydir}

function run_test {
    local testname=$1;
    local base=$(basename ${testname} .dl)

    ddlog -i ${base}.dl -L../../lib ${extraflags[${base}]}
    cd ${base}_ddlog
    cargo build
    cd ..
    if [ -f ${base}.dat ]; then
        ${base}_ddlog/target/debug/${base}_cli --no-print <${base}.dat >${base}.dump
        if [ -f ${base}.dump.expected ]; then
            diff -q ${base}.dump ${base}.dump.expected
        elif [ -f ${base}.dump.expected.gz ]; then
            gzip ${base}.dump
            zdiff -q ${base}.dump.gz ${base}.dump.expected.gz
        fi
    fi
    rm -rf ${base}_ddlog ${base}.dump ${base}.dump.gz
}

if [ $# == 0 ]; then
    tests=$(ls *.dl | grep -v fail)
else
    tests=$*
fi

index=1
count=$(echo ${tests} | wc -w)
echo "Running ${count} tests: ${tests}"
for i in ${tests}; do
    echo "Running test ${index}/${count}: ${i}"
    index=$((index+1))
    run_test $i
done
