#!/bin/bash

set -ex

# Tomcat version 9.X does not seem to work correctly as an embedded web server
TOMCATVERSION="8.5.2"
TOMCATLIB=`pwd`/apache-tomcat-${TOMCATVERSION}/lib
PROG=reach

stack install
if command clang -v 2>/dev/null; then
    export CC=clang
else
    export CC=gcc
fi

case `uname -s` in
    Darwin*)    SHLIBEXT=dylib; OPEN=open; JDK_OS=darwin;;
    Linux*)     SHLIBEXT=so; OPEN=xdg-open; JDK_OS=linux;;
    *)          echo "Unsupported OS"; exit -1
esac

if [ ! -d apache-tomcat-${TOMCATVERSION} ]; then
    echo "Installing apache Tomcat web server"
    wget http://archive.apache.org/dist/tomcat/tomcat-8/v${TOMCATVERSION}/bin/apache-tomcat-${TOMCATVERSION}.tar.gz
    tar xvfz apache-tomcat-${TOMCATVERSION}.tar.gz
    pushd apache-tomcat-${TOMCATVERSION}/webapps
    rm -rf ROOT*
    popd
    rm -rf apache-tomcat-${TOMCATVERSION}.tar.gz

    wget https://repo1.maven.org/maven2/org/apache/tomcat/tomcat-juli/${TOMCATVERSION}/tomcat-juli-${TOMCATVERSION}.jar
    mv tomcat-juli-${TOMCATVERSION}.jar apache-tomcat-${TOMCATVERSION}/lib
else
    echo "Tomcat already installed"
fi

# Build the Java library with the DDlog API.
make -C ..

# Compile the ${PROG}.dl DDlog program; use -j switch to generate FlatBuffers schema and Java bindings for it
ddlog -i ${PROG}.dl -j -L../../lib

# Compile the rust program; generates ../test/datalog_tests/${PROG}_ddlog/target/debug/lib${PROG}_ddlog.a
(cd ${PROG}_ddlog; cargo build --features=flatbuf)

# Create a shared library containing all the native code: ddlogapi.c, lib${PROG}_ddlog.a
${CC} -shared -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/${JDK_OS} -I../../rust/template -I../../lib ../ddlogapi.c -L${PROG}_ddlog/target/debug/ -l${PROG}_ddlog -o libddlogapi.${SHLIBEXT}

CLASSPATH=`pwd`/${PROG}_ddlog/flatbuf/java:`pwd`/../ddlogapi.jar:$CLASSPATH
# Compile generated Java classes (the FlatBuffer Java package must be compiled first and must be in the
# $CLASSPATH)
(cd ${PROG}_ddlog/flatbuf/java && javac `ls ddlog/__${PROG}/*.java` && javac `ls ddlog/${PROG}/*.java`)

set +x
for L in ${TOMCATLIB}/*.jar; do
    CLASSPATH=$L:$CLASSPATH
done

set -x
javac Main.java

${OPEN} http://localhost:8082/index.html
java -Djava.library.path=. Main

# Cleanup generated files
rm -rf *.class tomcat.8082 libddlogapi.so
# You can also delete the following
# rm -rf reach_ddlog apache-tomcat-${TOMCATVERSION}
