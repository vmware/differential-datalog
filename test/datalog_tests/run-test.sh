#!/bin/bash
# Run one datalog test

set -ex

function usage {
    echo "Usage: run-test.sh testname [debug|release]"
    echo "Run one Datalog test"
    echo "The following environment variables control this script:"
    echo "- DDLOGFLAGS controls the ddlog compilation process"
    echo "- RUSTFLAGS controls the Rust compiler flags"
    echo "- CARGOFLAGS controls the cargo (Rust package system) compilation flags"
    exit 1
}

if [ $# == 0 ]; then
    usage
fi

testname=$1
base=$(basename ${testname} .dl)
shift

build="release"
if [ $# == 1 ]; then
    build=$1
    shift
else
    usage
fi

CARGOFLAGS=""
if [ "x${build}" == "xrelease" ]; then
    CARGOFLAGS="--release ${CARGOFLAGS}"
elif [ "x${build}" == "xdebug" ]; then
    CARGOFLAGS="${CARGOFLAGS}"
else
    usage
fi

ddlog -i ${base}.dl -L../../lib ${DDLOGFLAGS}
cd ${base}_ddlog
cargo build ${CARGOFLAGS}
cd ..
if [ -f ${base}.dat ]; then
    ${base}_ddlog/target/${build}/${base}_cli --no-print <${base}.dat >${base}.dump
    if [ -f ${base}.dump.expected.gz ]; then
        zdiff -q ${base}.dump ${base}.dump.expected.gz
    elif [ -f ${base}.dump.expected ]; then
        diff -q ${base}.dump ${base}.dump.expected
    fi
fi
rm -rf ${base}.dump ${base}.dump.gz
