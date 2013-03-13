#javac -classpath   FoldJSON.java  -d out  -Xlint:deprecation  && jar -cvf taras.jar -C out/ . 
export HADOOP_USER_CLASSPATH_FIRST="true"
# this will need to change once more jars are added
export HADOOP_CLASSPATH=jython-2.7b1.jar:jyson-1.0.2.jar:akela-0.5-SNAPSHOT.jar
CP=$(HADOOP_CLASSPATH):/usr/lib/hbase/lib/hadoop-core.jar:/usr/lib/hive/lib/commons-cli-1.2.jar:/usr/lib/hbase/hbase-0.90.6-cdh3u4.jar
comma:=,
JAVA_SOURCE=WordCount.java PythonWrapper.java FoldJSON.java
TASK=FoldJSON
ARGS=input output

all: WordCount.jar

run: WordCount.jar
	java -cp WordCount.jar:$(CP) taras.$(TASK)

hadoop: WordCount.jar
	-hadoop fs -rmr /user/tglek/output
	time hadoop jar $< taras.$(TASK) -libjars $(subst :,$(comma),$(HADOOP_CLASSPATH)) $(ARGS)

WordCount.jar: out/CallJava.py
	javac  -Xlint:deprecation -d out  -cp $(CP) $(JAVA_SOURCE)
	jar -cvf $@ -C out .

out/CallJava.py: CallJava.py
	mkdir -p out/script
	ln $< $@

%.class: ../%.java
