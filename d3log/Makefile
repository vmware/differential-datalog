
EXEC = target/debug/mm3
all: out

mm_ddlog/types/mm.rs: mm.dl
	ddlog --action compile -L/Users/ehoffman/ddev/lib --d3log -i mm.dl

$(EXEC): mm_ddlog/types/mm.rs src/*.rs 
	cargo build

out: $(EXEC)
	$(EXEC) -n 3

clean:
	rm -rf mm_ddlog
