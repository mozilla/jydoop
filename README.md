=Purpose
Querying json with java is too much work. Doing in Python allows for easier local development + testing.

Idea is that one keeps boilerplate in java and does important things in python.

To test python map/reduce one can use FileDriver.py in __main__
For example:
```
python CallJava.py log > log.out
```
where log is a newline-separated json dump. See CallJava.py for an example a script runnable with normal python and jyson.


=Packaging
Python script gets wrapped into driver.jar which also contains HDFSDriver and HBaseDriver. Choose one depending on if you are doing a map of a hdfs or hbase.

To process files do:
````
make hadoop ARGS="input output" TASK=HDFSDriver SCRIPT=mypythonfile.py
````
To process hbase:
```
make hadoop ARGS="telemetry output 201302281 201302282 yyyyMMddk" SCRIPT=mypythonfile.py
```
python script has to define a map and (optionally) reduce function. If reduce function is not present hadoop will not do a reduce, which can save a lot of time for simple data dumps.

mypythonfile.py can be a file outside the tree.

Note, dependency jars can be fetched with
```
make download
```
