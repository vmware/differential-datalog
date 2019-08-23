#!/bin/sh

set -ex

retry() {
  cmd=$*
  $cmd || (sleep 2 && $cmd) || (sleep 10 && $cmd)
}

fetch_flatbuf() {
    rm -rf flatbuffers
    mkdir flatbuffers
    curl -L https://github.com/google/flatbuffers/archive/v1.11.0.tar.gz | tar -zx -C flatbuffers --strip-components 1
}

if [ "x`flatbuffers/flatc --version`" != "xflatc version 1.11.0" ]; then
    echo "Installing Flatbuf"
    retry fetch_flatbuf
    cd flatbuffers
    cmake -G "Unix Makefiles"
    make
    cd ..
    ln -f -s `pwd`/flatbuffers/flatc ~/.local/bin/flatc
fi
