#!/bin/bash
# This script is configured to be run by git pre-push

(cd rust/template/ && \
cargo fmt --all -- --check && \
cargo check --all)
(cd lib && rustfmt *.rs --check)
