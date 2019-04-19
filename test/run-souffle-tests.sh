#!/bin/bash
# Simple shell script which runs part of the souffle tests: just compilation, not Rust execution

set -e

for i in souffle*; do
    echo "Running $i"
    cd $i
    ../../tools/souffle-converter.py test.dl souffle.dl souffle.dat && ddlog -i souffle.dl -L ../../lib
    rm -f log
    cd ..
done
