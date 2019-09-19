#!/bin/bash
# Shell script to build and run a Java program
# tied to test/datalog_tests/span_string.dl program

set -ex

if command clang -v 2>/dev/null; then
    export CC=clang
else
    export CC=gcc
fi

PROG=span_string
CLASSPATH=$(pwd)/../../test/datalog_tests/${PROG}_ddlog/flatbuf/java:$(pwd)/../ddlogapi.jar:$CLASSPATH

case $(uname -s) in
    Darwin*)    SHLIBEXT=dylib; JDK_OS=darwin;;
    Linux*)     SHLIBEXT=so; JDK_OS=linux;;
    *)          echo "Unsupported OS"; exit 1
esac

# Compile the ${PROG}.dl DDlog program
ddlog -i ../../test/datalog_tests/${PROG}.dl -j -L../../lib
# Compile the rust program; generates ../test/datalog_tests/span_ddlog/target/release/libspan_ddlog.a
pushd ../../test/datalog_tests/${PROG}_ddlog
cargo build --release --features=flatbuf
popd

# Compile generated Java classes (the FlatBuffer Java package must be compiled first and must be in the
# $CLASSPATH)
(cd ../../test/datalog_tests/${PROG}_ddlog/flatbuf/java && javac $(ls ddlog/__"${PROG}"/*.java) && javac $(ls ddlog/"${PROG}"/*.java))

# Build the Java library with the DDlog API
make -C ..
# Compile SpanTest.java
javac SpanTest.java
# Create a shared library containing all the native code: ddlogapi.c, lib${PROG}_ddlog.a
${CC} -shared -fPIC -I"${JAVA_HOME}"/include -I"${JAVA_HOME}"/include/${JDK_OS} -I../../rust/template -I../../lib ../ddlogapi.c -L../../test/datalog_tests/${PROG}_ddlog/target/release/ -l${PROG}_ddlog -o libddlogapi.${SHLIBEXT}
# Run the java program pointing to the created shared library
java -Djava.library.path=. SpanTest > spantest.dump
diff spantest.dump spantest.dump.expected
# Cleanup generated files
rm -rf ./*.class libddlogapi.${SHLIBEXT}
