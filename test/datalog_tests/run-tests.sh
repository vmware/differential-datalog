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

# Make sure the ddlog executable is compiled and installed
# inside the test directory (don't override the executable in `~/.local/bin`).
stack install --local-bin-path ./bin
PATH=$(pwd)/bin:$PATH

if [ $# == 0 ]; then
    tests=$(ls *.dl | grep -v fail)
else
    tests=$*
fi

build=release
index=1
count=$(echo ${tests} | wc -w)
echo "Running ${count} tests: ${tests}"
for i in ${tests}; do
    echo "Running test ${index}/${count}: ${i}"
    base=$(basename ${i} .dl)
    if [ "x${RUSTFLAGS}" == "x" ]; then
        RUSTFLAGS="-C opt-level=z"
    fi
    DDLOGFLAGS="${DDLOGFLAGS} ${extraflags[$base]}"

    source ./run-test.sh $i ${build}
    index=$((index+1))
done
