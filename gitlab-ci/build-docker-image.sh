#!/bin/sh

# Script to compile Docker image for use with GitLab CI

set -e

if [ -z "$1" ]
then
    TAG="latest"
else
    TAG="$1"
fi

DIR=$(dirname "$0")
echo DIR is $DIR

docker build  --network=host -t ddlog/gitlab-ci:$TAG $DIR/../tools
echo Successfully created docker image.
echo To upload the image to dockerhub, run \"docker push ddlog/gitlab-ci:$TAG\".
echo To log into the image: \"docker run --network=host -it ddlog/gitlab-ci:$TAG\".
