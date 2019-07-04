#!/bin/bash
# Shell script to build and run the redist.dl program in Java
# This test is tied to the redist.dl program.  Note that
# the data files are not part of the repository, since they are too large;
# this test cannot run properly without these data files.

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

# Compile the redist.dl DDlog program
ddlog -i ../../test/datalog_tests/redist.dl -L../../lib
# Compile the rust program; generates ../test/datalog_tests/redist_ddlog/target/release/redist_ddlog.a and .so
pushd ../../test/datalog_tests/redist_ddlog
cargo build --release
popd
# Build the JAR with the DDlog API
make -C ..
# Compile RedistTest.java
javac -cp ..:fastutil-8.2.2.jar RedistTest.java
# Create a shared library containing all the native code: ddlogapi.c, libredist_ddlog.a
${CC} -shared -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/${JDK_OS} -I../../rust/template ../ddlogapi.c -I../../lib -L../../test/datalog_tests/redist_ddlog/target/release/ -lredist_ddlog -o libddlogapi.${SHLIBEXT}
# Run the java program pointing to the created shared library
# Note: this assumes that the fastutil-8.2.2.jar is in the current folder
java -Djava.library.path=. -cp ./fastutil-8.2.2.jar:../ddlogapi.jar:. RedistTest ../../test/datalog_tests/redist.dat > redist.java.dump
# Compare outputs
diff -q redist.java.dump ../../test/datalog_tests/redist.dump

# Cleanup
rm -rf *.class # redist.java.dump
