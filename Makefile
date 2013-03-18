#javac -classpath   HBaseDriver.java  -d out  -Xlint:deprecation  && jar -cvf taras.jar -C out/ . 
export HADOOP_USER_CLASSPATH_FIRST="true"
# this will need to change once more jars are added
export HADOOP_CLASSPATH=jython-2.7b1.jar:jyson-1.0.2.jar:akela-0.5-SNAPSHOT.jar
CP=$(HADOOP_CLASSPATH):/usr/lib/hbase/lib/hadoop-core.jar:/usr/lib/hive/lib/commons-cli-1.2.jar:/usr/lib/hbase/hbase-0.90.6-cdh3u4.jar
comma:=,
JAVA_SOURCE=HDFSDriver.java PythonWrapper.java HBaseDriver.java
TASK=HBaseDriver
ARGS=input output
SCRIPT=CallJava.py
all: driver.jar

run: driver.jar
	java -cp driver.jar:$(CP) taras.$(TASK)

hadoop: driver.jar
#	-hadoop fs -rmr /user/tglek/output
	time hadoop jar $< taras.$(TASK) -libjars $(subst :,$(comma),$(HADOOP_CLASSPATH)) $(ARGS)

driver.jar: out/CallJava.py $(JAVA_SOURCE)
	javac  -Xlint:deprecation -d out  -cp $(CP) $(JAVA_SOURCE)
	jar -cvf $@ -C out .

out/CallJava.py: $(SCRIPT)
	mkdir -p out/script
	ln -vf $< $@

%.class: ../%.java

download:
	wget http://repo1.maven.org/maven2/org/python/jython/2.7-b1/jython-2.7-b1.jar
	wget http://people.mozilla.org/~tglek/jyson-1.0.2.jar
