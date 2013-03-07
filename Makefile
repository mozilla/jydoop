#javac -classpath   FoldJSON.java  -d out  -Xlint:deprecation  && jar -cvf taras.jar -C out/ . 
CP=jython-2.7b1.jar:jyson-1.0.2.jar:/usr/lib/hbase/lib/hadoop-core.jar:/usr/lib/hive/lib/commons-cli-1.2.jar:/usr/lib/hbase/hbase-0.90.6-cdh3u4.jar:akela-0.5-SNAPSHOT.jar
export HADOOP_USER_CLASSPATH_FIRST="true"
# this will need to change once more jars are added
export HADOOP_CLASSPATH=jython-2.7b1.jar:jyson-1.0.2.jar
comma:=,

all: WordCount.jar

run: WordCount.jar
	java -cp WordCount.jar:$(CP) org.apache.hadoop.examples.WordCount

hadoop: WordCount.jar
	-hadoop fs -rmr /user/tglek/output
	time hadoop jar $< org.apache.hadoop.examples.WordCount -libjars $(subst :,$(comma),$(HADOOP_CLASSPATH)) input output

WordCount.jar: out/WordCount.class out/CallJava.py
	jar -cvf $@ -C out .

out/CallJava.py: CallJava.py
	mkdir -p out/script
	ln $< $@

%.class: ../%.java
	javac  -Xlint:deprecation -d out  -cp $(CP) $<
