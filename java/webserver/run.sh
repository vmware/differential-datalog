#!/bin/bash

set -ex

# Tomcat version 9.X does not seem to work correctly as an embedded web server
TOMCATVERSION="8.5.2"
TOMCATLIB=$(pwd)/apache-tomcat-${TOMCATVERSION}/lib

case $(uname -s) in
    Darwin*)    OPEN=open;;
    Linux*)     OPEN=xdg-open;;
    *)          echo "Unsupported OS"; exit 1
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

set +x
# This generates too much output
for L in ${TOMCATLIB}/*.jar; do
    CLASSPATH=$L:$CLASSPATH
done
set -x

source ../build_java.sh
compile reach.dl Main.java debug

${OPEN} http://localhost:8082/index.html
java -Djava.library.path=. Main

cleanup
rm -rf tomcat.8082
# You can also delete the following
# rm -rf reach_ddlog apache-tomcat-${TOMCATVERSION}
