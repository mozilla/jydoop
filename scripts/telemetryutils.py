"""Utilities for querying telemetry data using pydoop."""

# NOTE: When modifying this file, be careful to use java-specific imports
# only within setupjob, so that people can test scripts using python!

dateformat = 'yyyyMMdd'

def setupjob(job, args):
    """
    Set up a job to run on telemetry date ranges.

    Telemetry jobs expect two arguments, startdate and enddate, both in yyyymmdd format.
    """

    import java.text.SimpleDateFormat as SimpleDateFormat
    import java.util.Calendar as Calendar
    import com.mozilla.hadoop.hbase.mapreduce.MultiScanTableMapReduceUtil as MSTMRU
    import com.mozilla.util.Pair
    
    if len(args) != 2:
        raise Exception("Usage: <startdate-YYYYMMDD> <enddate-YYYYMMDD>")

    sdf = SimpleDateFormat(dateformat)
    startdate = Calendar.getInstance()
    startdate.setTime(sdf.parse(args[0]))
    enddate = Calendar.getInstance()
    enddate.setTime(sdf.parse(args[1]))

    print "startdate: %r enddate: %r" % (startdate, enddate)

    columns = [com.mozilla.util.Pair('data', 'json')]
    scans = MSTMRU.generateBytePrefixScans(startdate, enddate, dateformat,
                                           columns, 500, False)
    MSTMRU.initMultiScanTableMapperJob(
        'telemetry',
        scans,
        None, None, None, job)
