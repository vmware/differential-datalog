GO := go
BINDIR := $(CURDIR)/bin

export CGO_LDFLAGS
export CGO_CPPFLAGS

all: bin

# CGO_LDFLAGS = -L$(CURDIR)/pkg/ddlog/libs -lnetworkpolicy_controller_ddlog
# CGO_CPPFLAGS = -I$(CURDIR)/pkg/ddlog
check-env:
ifndef CGO_CPPFLAGS
	$(error CGO_CPPFLAGS is not set)
endif
ifndef CGO_LDFLAGS
	$(error CGO_LDFLAGS is not set)
endif

.PHONY: bin
bin: check-env
bin:
	@echo "===> Building binaries <==="
	GOBIN=$(BINDIR) $(GO) install github.com/vmware/differential-datalog/go/...

.PHONY: fmt
fmt:
	@echo "===> Formatting code <==="
	$(GO) fmt github.com/vmware/differential-datalog/go/...

# Run unit tests only, no integration tests
.PHONY: check-unit
check-unit: check-env
check-unit:
	@echo "===> Running unit tests <==="
	$(GO) test -v github.com/vmware/differential-datalog/go/pkg/...

.PHONY: check-bench
check-bench: check-env
check-bench:
	@echo "===> Running unit tests with benchmarks <==="
	$(GO) test -v -bench=. github.com/vmware/differential-datalog/go/pkg/...

.golangci-bin:
	@echo "===> Installing Golangci-lint <==="
	@curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $@ v1.21.0

.PHONY: golangci
# Unfortunately, it seems that ddlog.h is also required to run the linters
golangci: check-env
golangci: .golangci-bin
	@echo "===> Running Golangci-lint <==="
	@GOOS=linux .golangci-bin/golangci-lint run -c .golangci.yml

clean:
	rm -rf bin
	rm -rf .golangci-bin
