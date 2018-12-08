LIBS = libddlogapi.so
CLASS = ddlogapi/DDLogAPI.class
GCCFLAGS = -shared -fPIC

all: $(LIBS) $(CLASS)

libddlogapi.so: ddlogapi.c ddlogapi_DDLogAPI.h
	gcc $(GCCFLAGS) -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux -I../rust/template  $< -o $@

%.class: %.java
	javac $<

ddlogapi_DDLogAPI.h ddlogapi/DDLogApi.class: ddlogapi/DDLogAPI.java ddlogapi/DDLogCommand.java \
                         ddlogapi/DDLogRecord.java
	javac -h .  $<

clean:
	rm -f *.so *.class
