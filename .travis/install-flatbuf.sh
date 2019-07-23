#!/bin/sh

set -ex

travis_retry() {
  cmd=$*
  $cmd || (sleep 2 && $cmd) || (sleep 10 && $cmd)
}

fetch_flatbuf() {
    #curl -L https://github.com/google/flatbuffers/archive/v1.11.0.tar.gz | tar -zx;
    git clone https://github.com/google/flatbuffers.git
}

cd $HOME
travis_retry fetch_flatbuf
cd flatbuffers
cmake -G "Unix Makefiles"
make
ln -s $HOME/flatbuffers/flatc ~/.local/bin/flatc
