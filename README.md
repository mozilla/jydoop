jydoop: Efficient and Testable Hadoop map-reduce in Python
==========================================================

Purpose
-------

Querying Hadoop/HBase using custom java classes is complicated and tedious.
It's very difficult to test and debug analyses on small sets of sample data, or
without setting up a Hadoop/HBase cluster.

Writing analyses in Python allows for easier local development + testing
without having to set up Hadoop or HBase. The same analysis scripts can then be
deployed to a production cluster configuration.

Writing Scripts
---------------

The quickest way to get started is to have a look at some of the examples in
`scripts/`.

The minimal jydoop job requires only two function definitions:
- `map(key, value, context)` The map phase of MapReduce - called once for each
  input record, and data is written out by calling
  `context.write(new_key, new_value)`
- `setupjob(job, args)` Determines how data is made available to the job script
  and processes incoming arguments. Usually you will use an existing
  implementation of this function.

For most Mozilla data sources, there are predefined `setupjob` functions
available that you can reference in your script without implementing your own.
There are examples of this included in the `scripts/` dir.

In addition to the Mozilla data sources, there is also support for using the
output of one jydoop job as the input of another. More on this below.

Besides the required `map` and `setupjob` functions, there are a number of
optional functions you can implement for full MapReduce functionality:
- `reduce(key, values, context)` The reduce phase of MapReduce - called once
  for each key (as output by the Map phase) with a list of all values seen for
  that key. If you do not define a `reduce` function in your script, it will
  run as a Map-only job and skip the Reduce phase entirely.
- `combine(key, values, context)` An intermediate way of reducing partial value
  lists for a key. This function is entirely optional, but can improve
  performance if the logic for reducing values can be done in pieces. A good
  example of this is count-type jobs, where the overall `reduce` will still
  work fine even if some subsets of values have already been summed. The
  [Hadoop Documentation](http://wiki.apache.org/hadoop/HadoopMapReduce) has a
  nice description of the combine phase.
- `output(path, results)` You may override how data is written out to the
  destination file by implementing this method. The default behaviour is
  usually fine. The `results` argument is an iterator on the (key, value)
  pairs that come from the Reduce step.
- `mapsetup(context)` Called before the Map phase
- `mapcleanup(context)` Called after the Map phase
- `skip_local_output()` If this returns `True`, then output from the job is not
  downloaded from HDFS to a local file. The supplied output filename is used
  as the location in HDFS where data will be stored. If this function is
  omitted, the default behaviour is to output to a local text file, then remove
  any data from HDFS.

### Testing Locally
To test scripts, use locally saved sample data and FileDriver.py:
```
python FileDriver.py script/osdistribution.py sample.json analysis.out
```
where `sample.json` is a newline-separated json dump. See the examples in
`scripts/` for map-only or map-reduce jobs.

Local testing can be done on any machine with Python installed, and doesn't
require access to any extra libraries beyond what is included with jydoop, nor
does it require connectivity to the Hadoop cluster.

Production Setup
----------------

### Fetch resources

Fetch dependent JARs using
```
make download
```

Note: You may need to set the `http_proxy` environment variable to allow `curl`
to get out to the internet:
```
export http_proxy=http://myproxy:port
```

### Running as a Hadoop Job

Python scripts are wrapped into driver.jar with the Java driver.

For example, to count the distribution of operating systems on Mozilla
telemetry data for March 30th, 2013 run:
````
make hadoop ARGS="scripts/osdistribution.py outputfile 20130330 20130330"
````

Supported Types of Jobs
-----------------------

jydoop supports several different backend data sources which are of the
following types:
- HBase (mapper type `HBASE`)
- Plain sequence files (mapper type `TEXT`)
- Jydoop-formatted sequence files (mapper type `JYDOOP`)

The mapper type should be set by the `setupjob` function. Currently supported
types are `HBASE`, `TEXT`, or `JYDOOP`. The default is `HBASE` so you do not
need to specify this value for HBase data sources. Other mapper types should
set the `org.mozilla.jydoop.hbasecolumns` key in the job Configuration. For
example, the TestPilot `setupjob` function sets the mapper type using:
```
job.getConfiguration().set("org.mozilla.jydoop.mappertype", "TEXT")
```

All the different types require at least two arguments, namely the **script to
be run** and the **filename where output will be sent**.

### Telemetry

Telemetry jobs take two extra arguments, the **start date** and the
**end date** of the range you want to analyze. Telemetry data is quite large,
so it's best to keep the date range to a few days max.

The production example above uses the HBase support to access Telemetry data
(using the `telemetryutils.hbase_setupjob` setup function).

You may also access the most recent 14 days' data in HDFS. This will make jobs
finish somewhat more quickly than using HBase. To use the HDFS data, specify
the following in your script:
```
setupjob = telemetryutils.setupjob
```

Check the `osdistribution.hdfs.py` script for an example.

### Firefox Health Report Jobs

FHR jobs don't require any extra arguments beyond the script name and the
output file.

To help reduce the boilerplate required to write Firefox Health Report
jobs, a special decorator and Python class is made available. From your job
script:


    from healthreportutils import (
        FHRMapper,
        setupjob,
    )


    @FHRMapper(only_major_channels=True)
    def map(key, payload, context):
        if payload.telemetry_enabled:
            return

        for day, providers in payload.daily_data():
            pass


When the `@FHRMapper` decorator is applied to a *map* function, the 2nd
argument to the function will automatically be converted to a
`healthreportutils.FHRPayload` class. In addition, special arguments can
be passed to the decorator to perform common filtering operations
outside of your job.

See the source in `healthreportutils.py` for complete usage info.

### TestPilot

TestPilot jobs access data in Sequence Files in HDFS.

Scripts accessing TestPilot data require three arguments: the TestPilot study
name, the start date, and the end date. For example, to run the sample script
against the `testpilot_micropilot_search_study_2` study from June 10th to
June 17th, 2013:

```
make hadoop ARGS="scripts/testpilot_test.py test_out.txt testpilot_micropilot_search_study_2 2013-06-10 2013-06-17"
```


### Jydoop output -> jydoop input

You can run a jydoop job against the output of a previous jydoop job.

This enables workflows where you first filter or preprocess your input data and
store it back in HDFS, then write a number of different analysis scripts that
work on the filtered data set.

Normally jydoop jobs remove their output in HDFS once the data has been saved
locally.

If you want to keep the output in HDFS instead of saving locally, you can
implement the `skip_local_output` function in your job script (and have it
return `True`). This will cause the data not to be saved locally, and also
prevent it from being deleted from HDFS when the job is complete.

You then use the job's output in another job by using the `jydoop.setupjob`
function in your script.

As a simplistic example, if you have a two-stage job which first reads and
filters TestPilot data and stores the result into a HDFS location
`interesting_output`, then reads `interesting_output` to produce local data,
you could do the following:
```python
"""stage1.py - Filter TestPilot input for an interesting person"""
import json
import testpilotutils
import jydoop
def map(key, value, context):
    payload = json.loads(value)
    if payload["personid"] == "interesting!":
        context.write(key, value)

def skip_local_output():
    return True

setupjob = testpilotutils.setupjob
```

```python
"""stage2.py - Count events by type"""
import json
import jydoop
def map(key, value, context):
    payload = json.loads(value)
    for event in payload["events"]:
        context.write(event["type"], 1)

reduce = jydoop.sumreducer
combine = jydoop.sumreducer
setupjob = jydoop.setupjob
```

Run the jobs:
```
# Generate the HDFS output:
make hadoop ARGS="scripts/stage1.py interesting_output testpilot_study_1337 2013-06-10 2013-06-24"

# Process HDFS data and output local data:
make hadoop ARGS="scripts/stage2.py final_result.txt interesting_output"
```

You can then run any other jobs against `interesting_output` without having to
re-filter the original data.
