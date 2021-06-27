
EXEC = target/debug/mm3
all: out
DIST = ..
TARGET = rules

# skyline/$(TARGET).dl who is touching this file?
skyline/$(TARGET)_ddlog/types/$(TARGET).rs: skyline/$(TARGET).dl
	ddlog -L. -L$(DIST)/lib --d3log -i skyline/$(TARGET).dl

$(EXEC): skyline/$(TARGET)_ddlog/types/$(TARGET).rs src/*.rs 
	cargo build

out: $(EXEC)
	$(EXEC) # -i /users/ehoffman/record.dat.3

clean:
	rm -rf skyline/$(TARGET)_ddlog
