export HADOOP_USER_CLASSPATH_FIRST="true"
# this will need to change once more jars are added
export HADOOP_CLASSPATH=`pwd`/akela-0.5-SNAPSHOT.jar
hadoop jar taras.jar taras.FoldJSON  -libjars $HADOOP_CLASSPATH telemetry output 201201011  201201022 yyyyMMddk
