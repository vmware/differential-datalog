LIBS = libexternal.so libddlogapi.so
CLASS = Main.class external/External.class ddlogapi/DDLogAPI.class
GCCFLAGS = -shared -Wl,-soname,external -fPIC

all: $(LIBS) $(CLASS)

libexternal.so: external.c external_External.h
	gcc $(GCCFLAGS) -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux $< -o $@

libddlogapi.so: ddlogapi.c ddlogapi_DDLogAPI.h
	gcc $(GCCFLAGS) -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux $< -o $@

%.class: %.java
	javac $<

ddlogapi/DDLogApi.class: ddlogapi/DDLogAPI.java ddlogapi/DDLogCommand.java \
                         ddlogapi/DDLogRecord.java
	javac $<

ddlogapi_DDLogAPI.h: ddlogapi/DDLogAPI.class
	javah ddlogapi.DDLogAPI

external_External.h: external/External.class
	javah external.External

test: libexternal.so Main.class
	java -Djava.library.path=. Main

clean:
	rm -f *.so *.class external_External.h external/*.class
