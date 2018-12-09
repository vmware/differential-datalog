CLASS = ddlogapi/DDLogAPI.class

all: $(CLASS)

%.class: %.java
	javac $<

ddlogapi_DDLogAPI.h ddlogapi/DDLogApi.class: ddlogapi/DDLogAPI.java ddlogapi/DDLogCommand.java \
                         ddlogapi/DDLogRecord.java
	javac -h .  $<

clean:
	rm -f *.class ddlogapi/*.class
