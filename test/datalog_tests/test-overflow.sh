#!/bin/bash

#set -e
#we expect the next command to fail

RUSTFEATURES="checked_weights" ./run-test.sh overflow release
if [ $? -eq 0 ]; then
    echo "Test should have failed"
    false
fi
