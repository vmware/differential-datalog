#!/bin/bash
# Simple shell script which runs part of the souffle tests: just compilation, not Rust execution

set -e

#TESTS=souffle*
for i in $TESTS; do
    echo "Running $i"
    cd $i
    ../../tools/souffle-converter.py test.dl souffle && ddlog -i souffle.dl -L ../../lib
    rm -f log
    cd ..
done

# Tests imported from the souffle source tree
TESTS=access1
for i in $TESTS; do
    echo "Running $i"
    cd $i
    ../../tools/souffle-converter.py $i.dl souffle && ddlog -i souffle.dl -L ../../lib
    rm -f log
    cd ..
done
