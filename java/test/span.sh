#!/bin/bash
# Shell script to build and run the span program in Java

set -ex

# Compile the span.dl DDlog program; generates
# ../test/datalog_tests/span_ddlog/target/release/libspan_ddlog.a and .so
#stack test --ta '-p span'
# Build the Java library with the Ddlog API
make -C ..
# Force linking with the static library by deleting the dynamic library
rm -f ../../test/datalog_tests/span_ddlog/target/release/libspan_ddlog.so
# Compile SpanTest.java
javac -cp .. SpanTest.java Span.java
# Create manifest file for jar
mkdir -p META-INF
echo "Main-Class: SpanTest" > META-INF/MANIFEST.MF
# Create jar containing SpanTest.* classes and the Ddlog API
jar cmvf META-INF/MANIFEST.MF span.jar Span.class SpanTest*.class ../ddlogapi/*.class
rm -rf META-INF
# Create a shared library containing all the native code: ddlogapi.c, libspan_ddlog.a
gcc -shared -Wl,-soname,ddlogapi -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux -I../../rust/template ../ddlogapi.c ../ddlogapi_DDLogAPI.h -L../../test/datalog_tests/span_ddlog/target/release/ -lspan_ddlog -o libddlogapi.so
# Run the java program pointing to the created shared library
#java -Xcheck:jni -Djava.library.path=. -jar span.jar ../../test/datalog_tests/span.dat >span.java.dump
java -Djava.library.path=. -jar span.jar ../../test/datalog_tests/span.dat >span.java.dump
# Compare outputs
diff -q span.java.dump ../../test/datalog_tests/span.dump

# Cleanup
rm -rf META-INF *.class
