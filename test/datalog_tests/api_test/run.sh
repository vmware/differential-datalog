#!/bin/sh

# compile ../copy.dl, then compile and run api_test
stack test --ta "-p copy" && cargo run
