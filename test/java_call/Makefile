all: a.out

libexternal.so: external.c external_External.h
	gcc -shared -Wl,-soname,external -fPIC -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux $< -o $@

%.class: %.java
	javac $<

external_External.h: external/External.class
	javah external.External

test: libexternal.so Main.class
	java -Djava.library.path=. Main

clean:
	rm -f *.so *.class external_External.h external/*.class
