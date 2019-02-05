#!/bin/bash
# Shell script to build and run the span program in Java

set -ex

# Compile the span_uuid.dl DDlog program
ddlog -i ../../test/datalog_tests/span_uuid.dl -L../../lib
# Compile the rust program; generates ../test/datalog_tests/span_ddlog/target/release/libspan_ddlog.a and .so
pushd ../../test/datalog_tests/span_uuid_ddlog
cargo build --release
popd
# Build the Java library with the DDlog API
make -C ..
# Force linking with the static library by deleting the dynamic library
rm -f ../../test/datalog_tests/span_uuid_ddlog/target/release/libspan_uuid_ddlog.so
# Compile SpanTest.java
javac -cp .. SpanTest.java
# Create manifest file for jar
mkdir -p META-INF
echo "Main-Class: SpanTest" > META-INF/MANIFEST.MF
# Create jar containing SpanTest.* classes and the DDlog API
jar cmvf META-INF/MANIFEST.MF span.jar SpanTest*.class ../ddlogapi/*.class
rm -rf META-INF
# Create a shared library containing all the native code: ddlogapi.c, libspan_uuid_ddlog.a
gcc -shared -Wl,-soname,ddlogapi -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux -I../../rust/template ../ddlogapi.c ../ddlogapi_DDlogAPI.h -L../../test/datalog_tests/span_uuid_ddlog/target/release/ -lspan_uuid_ddlog -o libddlogapi.so
# Run the java program pointing to the created shared library
#java -Xcheck:jni -Djava.library.path=. -jar span.jar ../../test/datalog_tests/span_uuid.dat >span_uuid.java.dump
java -Djava.library.path=. -jar span.jar ../../test/datalog_tests/span_uuid.dat >span_uuid.java.dump
# Compare outputs
diff -q span_uuid.java.dump ../../test/datalog_tests/span_uuid.dump.expected

# Cleanup
rm -rf META-INF *.class
