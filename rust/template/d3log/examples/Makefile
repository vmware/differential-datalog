# how do the other test drivers access the executable?
ROOT = ../../../..

#DDLOG = $(ROOT)/ddlog/bin/ddlog
DDLOG=${HOME}/.local/bin/ddlog

all: run

compiler:
	(cd $(ROOT) ;  stack build && stack install)

program: 
	$(DDLOG) --no-staticlib --dynlib --action compile -L$(ROOT)/lib -L.. --d3log -i mini_skyline.dl

compile: 
	(cd mini_skyline_ddlog ; cargo build --features distribution)

start: 
	./mini_skyline_ddlog/target/debug/mini_skyline_cli

inc: compile start
run: compiler program compile start	
