# A Makefile containing recipes for working with the dbpedia_ownership D3log program.

# We use process substitution below, which is bash specific.
SHELL := /bin/bash

data/subsidiaries.ddlog.dat: SOURCE ?= data/dbpedia_ownership.csv
data/subsidiaries.ddlog.dat: LIMIT ?= 10000
data/subsidiaries.ddlog.dat: TXN_SIZE ?= 100
data/subsidiaries.ddlog.dat:
	@python3 data/generate.py $(SOURCE) $(LIMIT) $(TXN_SIZE) > $(@)

.PHONY: build
build:
	@cargo build --release

.PHONY: run
run: data/subsidiaries.ddlog.dat build
	cargo run --release -- run --member=127.0.0.1:5004 --members=127.0.0.1:5004

.PHONY: run-debug
run-debug: RUST_LOG ?= trace
run-debug: data/subsidiaries.ddlog.dat build
	@RUST_LOG="$(RUST_LOG)" cargo run --release -- run --member=127.0.0.1:5004 --members=127.0.0.1:5004

.PHONY: clean
clean:
	@-rm \
		data/subsidiaries.ddlog.dat \
		ownership.dump \
		&> /dev/null
