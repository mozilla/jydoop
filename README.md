#jydoop: Efficient and Testable hadoop map-reduce in Python

##Purpose
Querying hadoop/hbase using custom java classes is complicated and tedious. It's very difficult to test and debug analyses on small sets of sample data, or without setting up a Hadoop/Hbase cluster.

Writing analyses in Python allows for easier local development + testing without having to set up hadoop or hbase. The same analysis scripts can then be deployed to a cluster configuration.

##Writing Scripts
To test scripts, use locally saved sample data and FileDriver.py:
```
python FileDriver.py script/osdistribution.py saveddata > analysis.out
```
where `saveddata` is a newline-separated json dump. See the examples in `scripts/` for map-only or map-reduce jobs.

##Production Setup

Fetch dependent JARs using
```
make download
```

##Running an Job

Python scripts are wrapped into driver.jar with the Java driver.

For example, to count the distribution of operating systems on Mozilla telemetry data for 30-March-2013, run:
````
make ARGS="scripts/osdistribution.py outputfile 20130330 20133030" hadoop
````

## Firefox Health Report Jobs

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


When the @FHRMapper decorator is applied to a *map* function, the 2nd
argument to the function will automatically be converted to a
*healthreportutils.FHRPayload* class. In addition, special arguments can
be passed to the decorator to perform common filtering operations
outside of your job.

See the source in healthreportutils.py for complete usage info.

