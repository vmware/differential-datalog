# how do the other test drivers access the executable?
ROOT = ../../../..
DDLOG = $(ROOT)/ddlog/bin/ddlog
all: com

go:
	$(DDLOG) --no-staticlib --dynlib --action compile -L$(ROOT)/lib -L.. --d3log -i gather.dl

com: go
	(cd gather_ddlog ; cargo build --features distribution)
