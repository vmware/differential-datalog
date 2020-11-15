#!/bin/sh

set -ex

retry() {
  cmd=$*
  $cmd || (sleep 2 && $cmd) || (sleep 10 && $cmd)
}

fetch_flatbuf_unix() {
    rm -rf flatbuffers
    mkdir flatbuffers
    curl -L https://github.com/google/flatbuffers/archive/v1.11.0.tar.gz | tar -zx -C flatbuffers --strip-components 1
}

fetch_flatbuf_windows() {
    curl -L https://github.com/google/flatbuffers/releases/download/v1.11.0/flatc_windows_exe.zip > fb.zip && unzip fb.zip
}

echo "Installing Flatbuf"
if [ "x`flatbuffers/flatc --version`" != "xflatc version 1.11.0" ]; then
    if ( [ "$(uname)" = "Darwin" ] || [ "$(uname)" = "Linux" ] ); then
        retry fetch_flatbuf_unix
        cd flatbuffers
        cmake -G "Unix Makefiles"
        make
        cd ..
    else
        retry fetch_flatbuf_unix
        # On Windows, fetch pre-build executable instead of compiling from source.
        retry fetch_flatbuf_windows
    fi
fi

mkdir -p ~/.local/bin/
if ( [ "$(uname)" = "Darwin" ] || [ "$(uname)" = "Linux" ] ); then
    ln -f -s `pwd`/flatbuffers/flatc ~/.local/bin/flatc
else
    cp flatc.exe ~/.local/bin/
fi
