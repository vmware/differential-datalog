#!/bin/bash

set -e

echo Building DDlog distribution.
echo IMPORTANT: this script must be run in a clean copy of the DDlog source tree.

if [ `uname -s` = Darwin ]
then OS_NAME=osx
elif [ `uname -s` = Linux ]
then OS_NAME=linux
else
    echo Unsupported OS `uname -s` 1>&2
    exit 1
fi

if [ $# -eq 0 ]
  then
      TAG=
  else
      TAG=$1-
fi

# add date and OS name to the tag
export DIST_NAME=ddlog-$TAG$(date +'%Y%m%d%H%M%S')-$OS_NAME

# Directory for distribution files.
DIST_DIR=ddlog

rm -rf "$DIST_DIR"

# Step 1: Build DDlog binaries.
mkdir -p "$DIST_DIR/bin"
stack install --no-terminal --local-bin-path "$DIST_DIR/bin"

# Step 2: Add DDlog standard libraries to the distribution.
cp -r lib "$DIST_DIR/"

# Step 3: Include Rust dependencies for offline build.
(cd rust/template   &&
 # In addition to dependencies specified in `Cargo.toml`, add dependencies from
 # all `.toml` filesin the lib directory.
 cat Cargo.toml `find ../../lib/ -name "*.toml"` > Cargo.full.toml  &&
 mv Cargo.toml Cargo.toml.bak  &&

 # Backup original `Cargo.toml`.
 cp Cargo.full.toml Cargo.toml  &&

 # Set relative path to vendor directory in `.cargo/config`
 cargo vendor -s Cargo.toml > config.tmp &&

 # Restor `Cargo.toml`.
 mv Cargo.toml.bak Cargo.toml

 # The last line of config.tmp contains absolute path to the `vendor` directory,
 # which we don't want, so chop it off.
 if [ `uname -s` = Darwin ]; then ghead -n -1 config.tmp > config; else head -n -1 config.tmp > config; fi  &&

 # Use relative path instead.
 echo "directory = \"vendor\"" >> config    &&
 cp -r vendor "../../$DIST_DIR/"    &&
 mkdir "../../$DIST_DIR/.cargo" &&
 cp config "../../$DIST_DIR/.cargo/" &&
 cp Cargo.lock "../../$DIST_DIR/")

# Step 4: Add DDlog Java libraries.

# First copy Java sources only, then build and copy the .jar.
cp -r java "$DIST_DIR/"
cd java
make
cd ..
cp java/ddlogapi.jar "$DIST_DIR/java/"
cp java/ddlogapi_DDlogAPI.h "$DIST_DIR/java/"
cp -r doc "$DIST_DIR/"

# Step 5: Archive everything.
tar -czf "$DIST_NAME.tar.gz" "$DIST_DIR"

echo Distribution archive generated in $DIST_NAME.tar.gz
echo 'To install DDlog, extract the archive to your chosen installation directory and add ddlog/bin to $PATH.'
