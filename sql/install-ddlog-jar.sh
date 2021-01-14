#!/bin/sh

# When running in CI, the DDlog compiler should be prinstalled by the build stage.
if [ -z "${IS_CI_RUN}" ]; then
    stack install
fi

cd ../java
make
mvn install:install-file -Dfile=ddlogapi.jar -DgroupId=ddlog -DartifactId=ddlogapi -Dversion=0.1 -Dpackaging=jar
