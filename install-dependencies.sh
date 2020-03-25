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
curl https://sh.rustup.rs -sSf | sh -s -- --default-toolchain 1.41.1 -y
export PATH=$HOME/.cargo/bin:$PATH
rustup component add rustfmt
rustup component add clippy

./tools/install-flatbuf.sh
cd flatbuffers
export CLASSPATH=`pwd`"/java":$CLASSPATH
export PATH=`pwd`:$PATH
cd ..

GITDIR=$(git rev-parse --git-dir)
# Link to pre-commit script
ln -sf $(pwd)/tools/prepush.sh ${GITDIR}/hooks/pre-push
