#!/bin/bash
# Shell script to build and run a Java program
# tied to test/datalog_tests/span_string.dl program

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
ddlog -i ../../test/datalog_tests/span_string.dl -j -L../../lib
# Compile the rust program; generates ../test/datalog_tests/span_ddlog/target/release/libspan_ddlog.a and .so
pushd ../../test/datalog_tests/span_string_ddlog
cargo build --release
popd
# Build the Java library with the DDlog API
make -C ..
# Force linking with the static library by deleting the dynamic library
rm -f ../../test/datalog_tests/span_string_ddlog/target/release/libspan_string_ddlog.so
# Compile SpanTest.java and the generated Java file
javac -cp .. SpanTest.java Span_string.java
# Create a shared library containing all the native code: ddlogapi.c, libspan_string_ddlog.a
${CC} -shared -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/${JDK_OS} -I../../rust/template ../ddlogapi.c -L../../test/datalog_tests/span_string_ddlog/target/release/ -lspan_string_ddlog -o libddlogapi.${SHLIBEXT}
# Run the java program pointing to the created shared library
java -Djava.library.path=. -cp ../ddlogapi.jar:. SpanTest > spantest.dump
diff spantest.dump spantest.dump.expected
# Cleanup generated files
rm -rf *.class Span_string.java
