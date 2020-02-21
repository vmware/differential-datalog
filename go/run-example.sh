#!/usr/bin/env bash

set -e

source build-ddlog-prog.sh

make

./bin/example -record-commands cmds.txt -dump-changes changes.txt
