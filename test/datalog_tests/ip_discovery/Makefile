# A Makefile containing recipes for working with our example d3log program.

# We use process substitution below, which is bash specific.
SHELL := /bin/bash

transactions.ddlog.dat: SEED ?= 1
transactions.ddlog.dat: TXNS ?= 32768
transactions.ddlog.dat:
	@python3 ./generate.py $(SEED) $(TXNS) > $(@)

lsp.d3log.dat: transactions.ddlog.dat
	@grep -C 1 LogicalSwitchPort $(<) | grep -v '^--$$' | \
		sed 's/LogicalSwitchPort/ip_discovery_controller.LogicalSwitchPort/g' > $(@)

snooped.d3log.dat: transactions.ddlog.dat
	@grep -C 1 SnoopedAddress $(<) | grep -v '^--$$' | \
		sed 's/SnoopedAddress/ip_discovery_agent.SnoopedAddress/g' > $(@)

.PHONY: build
build:
	@cargo build --release

.PHONY: run-ctrl
run-ctrl: RUST_LOG ?= trace
run-ctrl: lsp.d3log.dat build
	@RUST_LOG="$(RUST_LOG)" cargo run --release -- \
		ctrl --member=127.0.0.1:5005 --members=127.0.0.1:5004 --members=127.0.0.1:5005 --lsp=$(<)

.PHONY: run-agent
run-agent: RUST_LOG ?= trace
run-agent: snooped.d3log.dat build
	@RUST_LOG="$(RUST_LOG)" cargo run --release -- \
		agent --member=127.0.0.1:5004 --members=127.0.0.1:5004 --members=127.0.0.1:5005 --snooped=$(<)

# This target helps in verifying that the produced transactions ultimately
# yield the same result when fed into a ddlog CLI program, meaning that both
# the non-distributed and distributed versions are semantically equivalent.
# Feeding the produced output back into the CLI requires some modifications to
# make EffectiveAddress and RealizedAddress input tables, though. In a
# nutshell, only these two input relations need to be present:
#
#  input relation EffectiveAddress(lsp: lspid_t, ip: ipaddr_t)
#  input relation RealizedAddress(lsp: lspid_t, timestamp: time_t, ip: ipaddr_t)
#
# Then generate the ddlog program with the --output-input-relations=Out switch.
# This setting will generate the corresponding output tables for the above
# input tables. The content of OutEffectiveAddress and OutRealizedAddress
# should then equal what the unmodified CLI program reports for the original
# transaction data.
.PHONY: check-equivalence
check-equivalence: effective.dump realized.dump
	@ls $(^) &> /dev/null || (echo "One of $(^) not found. Please make sure to have run the workload"; false)
	@cat $(^) <(echo 'dump OutEffectiveAddress; dump OutRealizedAddress;') | \
		sed -e 's/ip_discovery_controller.EffectiveAddress/EffectiveAddress/g' -e 's/ip_discovery_agent.RealizedAddress/RealizedAddress/g'


.PHONY: clean
clean:
	@-rm \
		effective.dump \
		lsp.d3log.dat \
		realized.dump \
		snooped.d3log.dat \
		transactions.dat \
		transactions.ddlog.dat \
		&> /dev/null
