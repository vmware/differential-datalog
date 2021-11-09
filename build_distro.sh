#!/bin/bash

set -xe

echo Building DDlog distribution.
echo IMPORTANT: this script must be run in a clean copy of the DDlog source tree.

if [ -z ${DIST_NAME} ]
then
    DIST_NAME=ddlog
fi

# Directory for distribution files.
DIST_DIR=ddlog

rm -rf "$DIST_DIR"

# Step 1: Build DDlog binaries.
mkdir -p "$DIST_DIR/bin"
stack install --no-terminal --local-bin-path "$DIST_DIR/bin"

# Step 2: Add DDlog standard libraries to the distribution.
cp -r lib "$DIST_DIR/"

# Step 3: Include Rust dependencies for offline build.
# We don't have space for this on the Windows runner.
if ! grep -q Microsoft /proc/version; then 
    cd rust/template

    # In addition to dependencies specified in `Cargo.toml`, add dependencies from
    # all `.toml` filesin the lib directory.
    cat Cargo.toml ../../lib/*.toml > Cargo.full.toml

    # Backup original `Cargo.toml`.
    mv Cargo.toml Cargo.toml.bak

    cp Cargo.full.toml Cargo.toml

    # Set relative path to vendor directory in `.cargo/config`
    cargo vendor -s Cargo.toml > config.tmp

    # Restore `Cargo.toml`.
    mv Cargo.toml.bak Cargo.toml

    # The last line of config.tmp contains absolute path to the `vendor` directory,
    # which we don't want, so chop it off.
    if [ `uname -s` = Darwin ]; then ghead -n -1 config.tmp > config; else head -n -1 config.tmp > config; fi

    # Use relative path instead.
    echo "directory = \"vendor\"" >> config
    # Move instead of copying, as we are running out of space
    # in the Github actions container.
    mv vendor "../../$DIST_DIR/"
    mkdir "../../$DIST_DIR/.cargo"
    cp config "../../$DIST_DIR/.cargo/"

    cd ../../
    cp Cargo.lock "$DIST_DIR/"
fi

# Step 4: Add DDlog Java libraries.

# First copy Java sources only, then build and copy the .jar.
cp -r java "$DIST_DIR/"
cd java
# Run `make clean` first in case the directory contains class files created by a
# different version of Java.
CLASSPATH=. make clean all
cd ..
cp java/ddlogapi.jar "$DIST_DIR/java/"
cp java/ddlogapi_DDlogAPI.h "$DIST_DIR/java/"
cp java/ddlogapi_DDlogAPI_DDlogCommandVector.h "$DIST_DIR/java/"
cp -r doc "$DIST_DIR/"

# Step 5: Archive everything.
tar -czf "$DIST_NAME.tar.gz" "$DIST_DIR"

echo Distribution archive generated in $DIST_NAME.tar.gz
echo 'To install DDlog, extract the archive to your chosen installation directory and add ddlog/bin to $PATH.'
