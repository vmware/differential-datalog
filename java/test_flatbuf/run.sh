#!/bin/bash
# Shell script to build and run a Java program
# tied to test/datalog_tests/$PROG.dl program

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

PROG=redist

# Compile the ${PROG}.dl DDlog program; use -j switch to generate FlatBuffers schema and Java bindings for it
ddlog -i ../../test/datalog_tests/${PROG}.dl -j -L../../lib
# Compile the rust program; generates ../test/datalog_tests/${PROG}_ddlog/target/release/lib${PROG}_ddlog.a
pushd ../../test/datalog_tests/${PROG}_ddlog
#cargo build --release
pushd java
# Invoke FlatBuffer compiler (must be in $PATH) to generate Java classes from the
# schema file
flatc --java --rust ${PROG}.fbs
# Compile generated Java classes (the FlatBuffer Java package must be compiled first and must be in the
# $CLASSPATH)
javac `ls ddlog/__${PROG}/*.java`
javac `ls ddlog/${PROG}/*.java`
popd
popd
# Build the Java library with the DDlog API
make -C ..
# Compile Test.java and the generated Java file
javac -cp "..:../../test/datalog_tests/${PROG}_ddlog/java:$CLASSPATH" Test.java
# Create a shared library containing all the native code: ddlogapi.c, lib${PROG}_ddlog.a
${CC} -shared -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/${JDK_OS} -I../../rust/template -I../../lib ../ddlogapi.c -L../../test/datalog_tests/${PROG}_ddlog/target/release/ -l${PROG}_ddlog -o libddlogapi.${SHLIBEXT}
# Run the java program pointing to the created shared library
java -Djava.library.path=. -cp ../ddlogapi.jar:.:../../test/datalog_tests/${PROG}_ddlog/java:$CLASSPATH Test > test.dump
diff test.dump test.dump.expected
# Cleanup generated files
rm -rf *.class
