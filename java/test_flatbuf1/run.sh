#!/bin/bash
# Compile and run tests related to flatbufTest.dl

set -ex

stack install
if command clang -v 2>/dev/null; then
    export CC=clang
else
    export CC=gcc
fi

case $(uname -s) in
    Darwin*)    SHLIBEXT=dylib; JDK_OS=darwin;;
    Linux*)     SHLIBEXT=so; JDK_OS=linux;;
    *)          echo "Unsupported OS"; exit 1
esac

if [ x$CLASSPATH == "x" ]; then
    echo "CLASSPATH is empty.  It should contain the flatbuf folder"
    exit 1;
fi

PROG=flatbufTest

# Build the Java library with the DDlog API.
make -C ..

CLASSPATH=`pwd`/${PROG}_ddlog/flatbuf/java:`pwd`/../ddlogapi.jar:$CLASSPATH

# Compile ${PROG}.dl program; use -j switch to generate FlatBuffers schema and Java bindings for it
ddlog -i ${PROG}.dl -j -L../../lib

# Compile the rust program; generates ../test/datalog_tests/${PROG}_ddlog/target/release/lib${PROG}_ddlog.a
(cd ${PROG}_ddlog; cargo build --release --features=flatbuf)

# Compile generated Java classes (the FlatBuffer Java package must be compiled first and must be in the
# $CLASSPATH)
(cd ${PROG}_ddlog/flatbuf/java && javac -Xlint:unchecked `ls ddlog/__${PROG}/*.java` && javac -Xlint:unchecked `ls ddlog/${PROG}/*.java`)

# Compile Test.java and the generated Java file
javac -Xlint:unchecked Test.java

# Create a shared library containing all the native code: ddlogapi.c, lib${PROG}_ddlog.a
${CC} -shared -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/${JDK_OS} -I../../rust/template -I../../lib ../ddlogapi.c -L${PROG}_ddlog/target/release/ -l${PROG}_ddlog -o libddlogapi.${SHLIBEXT}

# Run the java program pointing to the created shared library
java -Djava.library.path=. Test > test.dump
diff fb.dump rec.dump
diff fb.dump fb.dump.expected

# Cleanup generated files
rm -rf *.class
