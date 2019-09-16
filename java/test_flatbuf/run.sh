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

# Build the Java library with the DDlog API.
make -C ..

CLASSPATH=`pwd`/../../test/datalog_tests/${PROG}_ddlog/flatbuf/java:`pwd`/../ddlogapi.jar:$CLASSPATH

# Compile the ${PROG}.dl DDlog program; use -j switch to generate FlatBuffers schema and Java bindings for it
ddlog -i ../../test/datalog_tests/${PROG}.dl -j -L../../lib

# Compile the rust program; generates ../test/datalog_tests/${PROG}_ddlog/target/debug/lib${PROG}_ddlog.a
(cd ../../test/datalog_tests/${PROG}_ddlog; cargo build --features=flatbuf)

# Compile generated Java classes (the FlatBuffer Java package must be compiled first and must be in the
# $CLASSPATH)
(cd ../../test/datalog_tests/${PROG}_ddlog/flatbuf/java && javac `ls ddlog/__${PROG}/*.java` && javac `ls ddlog/${PROG}/*.java`)

# Compile Test.java and the generated Java file
javac Test.java

# Create a shared library containing all the native code: ddlogapi.c, lib${PROG}_ddlog.a
${CC} -shared -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/${JDK_OS} -I../../rust/template -I../../lib ../ddlogapi.c -L../../test/datalog_tests/${PROG}_ddlog/target/debug/ -l${PROG}_ddlog -o libddlogapi.${SHLIBEXT}

# Run the java program pointing to the created shared library
java -Djava.library.path=. Test > test.dump
diff test.dump test.dump.expected

# Cleanup generated files
rm -rf *.class
