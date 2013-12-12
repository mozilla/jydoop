HBASE_CP ?= $(shell hbase classpath)
HADOOP_CP ?= $(shell hadoop classpath)
# this will need to change once more jars are added
JACKSON_CP=jython-standalone-2.7-b1.jar:akela-0.5-SNAPSHOT.jar:jackson-core-2.1.4.jar:jackson-databind-2.1.4.jar:jackson-annotations-2.1.4.jar

export HADOOP_USER_CLASSPATH_FIRST="true"
export HADOOP_CLASSPATH := $(JACKSON_CP)
CP=$(HADOOP_CP):$(HBASE_CP):$(JACKSON_CP)
comma:=,
JAVA_SOURCE=$(addprefix org/mozilla/jydoop/,PythonWrapper.java PythonValue.java PythonKey.java HadoopDriver.java JacksonWrapper.java PySerializer.java)
TASK=HadoopDriver
ARGS=input output
TEST_PY=test.py
all: driver.jar

check: driver.jar
	java -cp driver.jar:$(CP) org.mozilla.jydoop.PythonWrapper $(TEST_PY)

python: driver.jar
	java -cp driver.jar:$(CP) org.python.util.jython

run: driver.jar
	java -cp driver.jar:$(CP) org.mozilla.jydoop.$(TASK)

hadoop: driver.jar
	time hadoop jar $< org.mozilla.jydoop.$(TASK) -libjars $(subst :,$(comma),$(JACKSON_CP)) $(ARGS)

out/pylib:
	mkdir -p out
	ln -s ../pylib out/pylib

out/scripts:
	mkdir -p out
	ln -s ../scripts out/scripts

driver.jar: out/scripts out/pylib $(wildcard pylib/*.py scripts/*.py scripts/fhr/*.py) $(JAVA_SOURCE)
	javac -Xlint:deprecation -d out  -cp $(CP) $(JAVA_SOURCE)
	jar -cvf $@ -C out .

%.class: ../%.java

download:
	wget -c http://repo1.maven.org/maven2/org/python/jython-standalone/2.7-b1/jython-standalone-2.7-b1.jar -O jython-standalone-2.7-b1.jar
	wget -c http://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/2.1.4/jackson-core-2.1.4.jar -O jackson-core-2.1.4.jar
	wget -c http://people.mozilla.org/~bsmedberg/akela-0.5-SNAPSHOT.jar -O akela-0.5-SNAPSHOT.jar
	wget -c http://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.1.4/jackson-databind-2.1.4.jar -O jackson-databind-2.1.4.jar
	wget -c http://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-annotations/2.1.4/jackson-annotations-2.1.4.jar -O jackson-annotations-2.1.4.jar
