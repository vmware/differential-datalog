#!/bin/bash
# Script that installs the dependencies needed to build and run Differential Dataflow

echo "Installing dependencies for DDlog"
echo "This script should be invoked with '. ./install-dependencies.sh' to set up the environment properly"

case "$OSTYPE" in
    linux*) ;;
    *) echo "Unhandled operating system $OSTYPE"; exit 1;;
esac


echo "Installing Haskell"
curl -sSL https://get.haskellstack.org/ | sh

echo "Installing Rust"
curl https://sh.rustup.rs -sSf | sh -s - -y
export PATH=$HOME/.cargo/bin:$PATH

echo "Installing Flatbuf"
if [ ! -d flatbuffers-1.11.0 ]; then
    wget https://github.com/google/flatbuffers/archive/v1.11.0.tar.gz
    tar xvfz v1.11.0.tar.gz
    rm -rf v1.11.0.tar.gz
fi
pushd flatbuffers-1.11.0
cmake -G "Unix Makefiles" -DCMAKE_BUILD_TYPE=Release
make -j3
export CLASSPATH=`pwd`"/java":$CLASSPATH
export PATH=`pwd`:$PATH
popd
