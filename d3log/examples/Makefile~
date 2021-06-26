
EXEC = target/debug/mm3
all: out
DIST = ..

mm_ddlog/types/mm.rs: mm.dl d3.dl
	ddlog --no-staticlib --dynlib --action compile -L$(DIST)/lib --d3log -i mm.dl

$(EXEC): mm_ddlog/types/mm.rs src/*.rs 
	cargo build

out: $(EXEC)
	$(EXEC) -n 3

clean:
	rm -rf mm_ddlog
