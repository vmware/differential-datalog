#!/bin/sh

FLATBUF_VERSION="2.0.0"
set -ex

retry() {
  cmd=$*
  $cmd || (sleep 2 && $cmd) || (sleep 10 && $cmd)
}

fetch_flatbuf_unix() {
    git clone https://github.com/google/flatbuffers.git --branch v${FLATBUF_VERSION}
}

fetch_flatbuf_windows() {
    curl -L https://github.com/google/flatbuffers/releases/download/v${FLATBUF_VERSION}/Windows.flatc.binary.zip > fb.zip && unzip fb.zip
}

echo "Installing Flatbuf"
if [ "x`flatbuffers/flatc --version`" != "xflatc version ${FLATBUF_VERSION}" ]; then
    if ( [ "$(uname)" = "Darwin" ] || [ "$(uname)" = "Linux" ] ); then
        retry fetch_flatbuf_unix
        cd flatbuffers
        cmake -G "Unix Makefiles"

        ncores=1
        if ( [ "$(uname)" = "Linux" ] ); then
            ncores=$(nproc)
        elif ( [ "$(uname)" = "Darwin" ] ); then
            ncores=$(sysctl -n hw.logicalcpu)
        fi
        make -j ${ncores}

        cd ..
    else
        retry fetch_flatbuf_unix
        # On Windows, fetch pre-built executable instead of compiling from source.
        retry fetch_flatbuf_windows
    fi
fi

mkdir -p ~/.local/bin/
export PATH=~/.local/bin:$PATH

if ( [ "$(uname)" = "Darwin" ] || [ "$(uname)" = "Linux" ] ); then
    ln -f -s `pwd`/flatbuffers/flatc ~/.local/bin/flatc
else
    cp flatc.exe ~/.local/bin/
fi
