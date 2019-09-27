#!/bin/bash
# Shell script which generates a Java program from a DDlog program and compiles it
# The following environment variables are used

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

function compile {
    local DLFILE=$1   # basename ddlog program that is being compiled (without .dl suffix)
    local JAVAPROG=$2 # java test program that is being compiled
    local BUILD=$3    # one of "debug" or "release"

    if [ "x${BUILD}" != "xdebug" -a "x${BUILD}" != "xrelease" ]; then
        echo "Third argument of compile must be 'debug' or 'release', not ${BUILD}"
        exit 1
    fi

    local DLPROG=$(basename ${DLFILE} .dl)
    local DLDIR=$(dirname ${DLFILE})
    if [ "x${DLDIR}" == "x" ]; then
        DLDIR="."
    fi

    CLASSPATH=$(pwd)/${DLDIR}/${DLPROG}_ddlog/flatbuf/java:$(pwd)/../ddlogapi.jar:..:$CLASSPATH
    # Compile the span_uuid.dl DDlog program
    ddlog -i ${DLFILE} -L../../lib -j
    # Compile the rust program; generates ../test/datalog_tests/span_ddlog/target/debug/libspan_ddlog.a
    pushd ${DLDIR}/${DLPROG}_ddlog
    if [ "x${BUILD}" == "xrelease" ]; then
        cargo build --features=flatbuf --release
    else
        cargo build --features=flatbuf
    fi
    popd
    # Build the Java library with the DDlog API
    make -C ..

    (cd ${DLDIR}/${DLPROG}_ddlog/flatbuf/java && javac $(ls ddlog/__${DLPROG}/*.java) && javac $(ls ddlog/${DLPROG}/*.java))

    # Compile Java program
    javac -encoding utf8 -Xlint:unchecked ${JAVAPROG}
    # Create a shared library containing all the native code: ddlogapi.c, ${DLPROG}_ddlog.a
    ${CC} -shared -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/${JDK_OS} -I../../rust/template -I../../lib ../ddlogapi.c -L${DLDIR}/${DLPROG}_ddlog/target/${BUILD}/ -l${DLPROG}_ddlog -o libddlogapi.${SHLIBEXT}
}

function cleanup {
    rm -rf ./*.class libddlogapi.${SHLIBEXT}
}
