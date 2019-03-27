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
# Build the Java library with the DDlog API
make -C ..
# Force linking with the static library by deleting the dynamic library
rm -f ../../test/datalog_tests/redist_ddlog/target/release/libredist_ddlog.so
# Compile RedistTest.java
javac -cp .. RedistTest.java
# Create manifest file for jar
mkdir -p META-INF
echo "Main-Class: RedistTest" > META-INF/MANIFEST.MF
# Create jar containing RedistTest.* classes and the DDlog API
jar cmvf META-INF/MANIFEST.MF redist.jar RedistTest*.class ../ddlogapi/*.class
rm -rf META-INF
# Create a shared library containing all the native code: ddlogapi.c, libredist_ddlog.a
${CC} -shared -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/${JDK_OS} -I../../rust/template ../ddlogapi.c -L../../test/datalog_tests/redist_ddlog/target/release/ -lredist_ddlog -o libddlogapi.${SHLIBEXT}
# Run the java program pointing to the created shared library
#java -Xcheck:jni -Djava.library.path=. -jar redist.jar ../../test/datalog_tests/redist.dat >redist.java.dump
java -Djava.library.path=. -jar redist.jar ../../test/datalog_tests/redist.dat > redist.java.dump
# Compare outputs
diff -q redist.java.dump ../../test/datalog_tests/redist.dump

# Cleanup
rm -rf META-INF *.class
