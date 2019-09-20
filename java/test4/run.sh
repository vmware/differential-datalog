#!/bin/bash
# Shell script to build and run a Java program tied to ./x.dl

set -ex

if command clang -v 2>/dev/null; then
    export CC=clang
else
    export CC=gcc
fi

PROG=x
CLASSPATH=$(pwd)/${PROG}_ddlog/flatbuf/java:$(pwd)/../ddlogapi.jar:$CLASSPATH

case $(uname -s) in
    Darwin*)    SHLIBEXT=dylib; JDK_OS=darwin;;
    Linux*)     SHLIBEXT=so; JDK_OS=linux;;
    *)          echo "Unsupported OS"; exit 1
esac

# Compile the span_uuid.dl DDlog program
ddlog -i ${PROG}.dl -j -L../../lib
# Compile the rust program; generates ${PROG}_ddlog/target/debug/lib${PROG}_ddlog.a
pushd ${PROG}_ddlog
cargo build --features=flatbuf
popd

# Build the Java library with the DDlog API
make -C ..

# Compile generated Java classes (the FlatBuffer Java package must be compiled first and must be in the
# $CLASSPATH)
(cd ${PROG}_ddlog/flatbuf/java && javac $(ls ddlog/__"${PROG}"/*.java) && javac $(ls ddlog/"${PROG}"/*.java))

# Compile XTest.java
javac XTest.java
# Create a shared library containing all the native code: ddlogapi.c, libspan_string_ddlog.a
${CC} -shared -fPIC -I"${JAVA_HOME}"/include -I${JAVA_HOME}/include/${JDK_OS} -I../../rust/template -I../../lib ../ddlogapi.c -L${PROG}_ddlog/target/debug/ -l${PROG}_ddlog -o libddlogapi.${SHLIBEXT}
# Run the java program pointing to the created shared library
java -Djava.library.path=. XTest > xtest.dump
diff xtest.dump xtest.dump.expected
# Cleanup generated files
rm -rf ./*.class libddlogapi.${SHLIBEXT}
