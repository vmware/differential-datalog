#!/bin/sh

set -ex

travis_retry() {
  cmd=$*
  $cmd || (sleep 2 && $cmd) || (sleep 10 && $cmd)
}

fetch_flatbuf() {
    rm -rf flatbuffers
    mkdir flatbuffers
    curl -L https://github.com/google/flatbuffers/archive/v1.11.0.tar.gz | tar -zx -C flatbuffers --strip-components 1;
    #git clone https://github.com/google/flatbuffers.git
}

cd $HOME

if [ "x`flatbuffers/flatc --version`" != "xflatc version 1.11.0" ]; then
    travis_retry fetch_flatbuf
    cd flatbuffers
    cmake -G "Unix Makefiles"
    make
fi

ln -f -s $HOME/flatbuffers/flatc ~/.local/bin/flatc
