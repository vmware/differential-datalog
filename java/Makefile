CLASS = ddlogapi/DDlogAPI.class ddlogapi/DDlogCommand.class ddlogapi/DDlogRecord.class

all: $(CLASS)

%.class: %.java ddlogapi_DDlogAPI.h
	javac $<

ddlogapi_DDlogAPI.h: ddlogapi/DDlogAPI.java
	javac -h . $<

clean:
	rm -f *.class ddlogapi/*.class
