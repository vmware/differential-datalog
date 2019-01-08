CLASS = ddlogapi/DDLogAPI.class ddlogapi/DDLogCommand.class ddlogapi/DDLogRecord.class

all: $(CLASS)

%.class: %.java ddlogapi_DDLogAPI.h
	javac $<

ddlogapi_DDLogAPI.h: ddlogapi/DDLogAPI.java
	javac -h . $<

clean:
	rm -f *.class ddlogapi/*.class
