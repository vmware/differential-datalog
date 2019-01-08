CLASS = ddlogapi/DDLogAPI.class

all: $(CLASS)

%.class: %.java ddlogapi_DDLogAPI.h
	javac $<

ddlogapi_DDLogAPI.h ddlogapi/DDLogApi.class: ddlogapi/DDLogAPI.java ddlogapi/DDLogCommand.java \
                         ddlogapi/DDLogRecord.java
	javac -h .  $<

clean:
	rm -f *.class ddlogapi/*.class
