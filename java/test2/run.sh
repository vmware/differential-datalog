#!/bin/bash
# Shell script to build and run the a combination of two DDlog programs
# in the same process

set -ex

if command clang -v 2>/dev/null; then
    export CC=clang
else
    export CC=gcc
fi

case `uname -s` in
    Darwin*)    SHLIBEXT=dylib; JDK_OS=darwin;;
    Linux*)     SHLIBEXT=so; JDK_OS=linux;;
    *)          echo "Unsupported OS"; exit -1
esac

# Compile the span_uuid.dl DDlog program
ddlog -i ./two.dl -L../../lib
# Compile the rust program; generates two_ddlog/target/debug/two_ddlog.a and .so
pushd two_ddlog
cargo build
popd
# Build the Java library with the DDlog API
make -C ..
# Compile TwoTest.java
javac -cp .. TwoTest.java
# Create a shared library containing all the native code: ddlogapi.c, libtwo_ddlog.a
${CC} -shared -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/${JDK_OS} -I../../rust/template -I../../lib ../ddlogapi.c -Ltwo_ddlog/target/debug/ -ltwo_ddlog -o libddlogapi.${SHLIBEXT}
# Run the java program pointing to the created shared library
java -Djava.library.path=. -cp ../ddlogapi.jar:. TwoTest

# Cleanup
rm -rf *.class
