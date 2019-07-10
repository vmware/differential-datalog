#!/bin/bash
# Shell script to build and run a Java program tied to ./x.dl

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
ddlog -i x.dl -j -L../../lib
# Compile the rust program; generates x_ddlog/target/debug/libx_ddlog.a
pushd x_ddlog
cargo build
popd
# Build the Java library with the DDlog API
make -C ..
# Compile XTest.java and the generated Java file
javac -cp .. XTest.java X.java
# Create a shared library containing all the native code: ddlogapi.c, libspan_string_ddlog.a
${CC} -shared -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/${JDK_OS} -I../../rust/template -I../../lib ../ddlogapi.c -Lx_ddlog/target/debug/ -lx_ddlog -o libddlogapi.${SHLIBEXT}
# Run the java program pointing to the created shared library
java -Djava.library.path=. -cp ../ddlogapi.jar:. XTest > xtest.dump
diff xtest.dump xtest.dump.expected
# Cleanup generated files
rm -rf *.class X.java
