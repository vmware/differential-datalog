JAR=ddlogapi.jar
# There are other *.class files generated, but
# the remaining ones are for inner classes of these
# so they are always generated together.
CLASS = ddlogapi/DDlogAPI.class \
		ddlogapi/DDlogCommand.class \
		ddlogapi/DDlogRecord.class \
		ddlogapi/DDlogRecCommand.class \
		ddlogapi/DDlogException.class

all: ${JAR}

${JAR}: ${CLASS}
	jar cvf ${JAR} ddlogapi/*.class

%.class: %.java ddlogapi_DDlogAPI.h
	javac $<

ddlogapi_DDlogAPI.h: ddlogapi/DDlogAPI.java
	javac -h . $<

clean:
	rm -f *.class ddlogapi/*.class
