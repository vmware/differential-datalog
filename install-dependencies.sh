#!/bin/bash
# Script that installs the dependencies needed to build and run Differential Dataflow

echo "Installing dependencies for DDlog"
echo "This script should be invoked with '. ./install-dependencies.sh' to set up the environment properly"

case "$OSTYPE" in
    linux*) ;;
    osx*) ;;
    *) echo "Unhandled operating system $OSTYPE"; exit 1;;
esac


echo "Installing Haskell"
./tools/install-stack.sh

echo "Installing Rust"
curl https://sh.rustup.rs -sSf | sh -s - -y
export PATH=$HOME/.cargo/bin:$PATH

./tools/install-flatbuf.sh
pushd flatbuffers
export CLASSPATH=`pwd`"/java":$CLASSPATH
export PATH=`pwd`:$PATH
popd
