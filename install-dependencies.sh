#!/bin/bash
# Script that installs the dependencies needed to build and run Differential Datalog

echo "Installing dependencies for DDlog"
echo "This script should be invoked with '. ./install-dependencies.sh' to set up the environment properly"

case "$OSTYPE" in
    "linux*") sudo apt install libgoogle-perftools-dev ;;
    "osx*") ;;
    "*") echo "Unhandled operating system $OSTYPE"; exit 1;;
esac

RUST_VERSION="1.52.1"

echo "Installing Haskell"
./tools/install-stack.sh

echo "Installing Rust"
curl https://sh.rustup.rs -sSf | sh -s -- --default-toolchain ${RUST_VERSION} -y
export PATH=$HOME/.cargo/bin:$PATH

# If another version is installed the previous line does not do anything
rustup default ${RUST_VERSION}
rustup toolchain install ${RUST_VERSION}
rustup component add rustfmt
rustup component add clippy

pip3 install parglare==0.12.0

sudo apt install default-jdk

./tools/install-flatbuf.sh
cd flatbuffers
export CLASSPATH=`pwd`"/java":$CLASSPATH
export PATH=`pwd`:$PATH
cd ..

GITDIR=$(git rev-parse --git-dir)
# Link to pre-commit script
ln -sf $(pwd)/tools/prepush.sh ${GITDIR}/hooks/pre-push
