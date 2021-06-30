# how do the other test drivers access the executable?
ROOT = ../../../..
DDLOG = $(ROOT)/ddlog/bin/ddlog
all: run

compiler:
	(cd $(ROOT) ;  stack build )

program: 
	$(DDLOG) --no-staticlib --dynlib --action compile -L$(ROOT)/lib -L.. --d3log -i gather.dl

compile: 
	(cd gather_ddlog ; cargo build --features distribution)

start: 
	./gather_ddlog/target/debug/deps/gather_cli

inc: compile start
run: compiler program compile start	
