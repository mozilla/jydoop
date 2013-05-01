HBASE_PATH ?= /usr/lib/hbase
SPACE := $(NULL) $(NULL)
HBASE_CP = $(subst $(SPACE),:,$(wildcard $(HBASE_PATH)/*.jar) $(wildcard $(HBASE_PATH)/lib/*.jar))

#javac -classpath   HBaseDriver.java  -d out  -Xlint:deprecation  && jar -cvf taras.jar -C out/ . 
export HADOOP_USER_CLASSPATH_FIRST="true"
# this will need to change once more jars are added
export HADOOP_CLASSPATH=jython-standalone-2.7-b1.jar:akela-0.5-SNAPSHOT.jar:jackson-core-2.1.4.jar
CP=$(HADOOP_CLASSPATH):$(HBASE_CP)
comma:=,
JAVA_SOURCE=$(addprefix org/mozilla/jydoop/,PythonWrapper.java PythonValue.java PythonKey.java HBaseDriver.java JacksonWrapper.java)
TASK=HBaseDriver
ARGS=input output
SCRIPT=$(error Must specify SCRIPT=)
TEST_PY=test.py
all: driver.jar

check: driver.jar
	java -cp driver.jar:$(CP) org.mozilla.jydoop.PythonWrapper $(TEST_PY)

run: driver.jar
	java -cp driver.jar:$(CP) org.mozilla.jydoop.$(TASK)

hadoop: driver.jar
	time hadoop jar $< org.mozilla.jydoop.$(TASK) -libjars $(subst :,$(comma),$(HADOOP_CLASSPATH)) $(ARGS)

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
