"""
Utilities for querying crash-stats data using jydoop.
"""

# NOTE: When modifying this file, be careful to use java-specific imports
# only within setupjob, so that people can test scripts using python!

dateformat = 'yyMMdd'

def dosetupjob(columnlist):
    """
    Returns a setupjob function which retrieves the specified columns.
    Expects columns to be [('family', 'qualifier')...]
    """
    
    def setupjob(job, args):
        """
        Set up a job to run on crash-stats date ranges.

        Expects three arguments:
          startdate (yymmdd)
          enddate (yymmdd)
        """

        import java.text.SimpleDateFormat as SimpleDateFormat
        import java.util.Calendar as Calendar
        import com.mozilla.hadoop.hbase.mapreduce.MultiScanTableMapReduceUtil as MSTMRU
        from com.mozilla.util import Pair

        if len(args) != 2:
            raise Exception("Usage: <startdate-yymmdd> <enddate-yymmdd>")

        startarg, endarg = args

        sdf = SimpleDateFormat(dateformat)
        startdate = Calendar.getInstance()
        startdate.setTime(sdf.parse(startarg))
        enddate = Calendar.getInstance()
        enddate.setTime(sdf.parse(endarg))

        columns = [Pair(family, qualifier) for family, qualifier in columnlist]

        scans = MSTMRU.generateHexPrefixScans(startdate, enddate, dateformat,
                                              columns, 500, False)
        MSTMRU.initMultiScanTableMapperJob(
            'crash_reports',
            scans,
            None, None, None, job)

        # inform HBaseDriver about the columns we expect to receive
        job.getConfiguration().set("org.mozilla.jydoop.hbasecolumns",
                                   ','.join(':'.join(column) for column in columnlist))

    return setupjob
