"""Utilities for querying testpilot data using jydoop."""

dateformat = 'yyyy-MM-dd'

# study_name, date
pathformat = '/bagheera/testpilot_%s/%s'

def setupjob(job, args):
    """
    Set up a job to run on a date range of directories.

    Jobs expect two arguments, startdate and enddate, both in yyyy-MM-dd format.
    """

    import java.text.SimpleDateFormat as SimpleDateFormat
    import java.util.Date as Date
    import java.util.Calendar as Calendar
    import com.mozilla.util.DateUtil as DateUtil
    import com.mozilla.util.DateIterator as DateIterator
    import org.apache.hadoop.mapreduce.lib.input.FileInputFormat as FileInputFormat
    import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat as MyInputFormat

    if len(args) != 3:
        raise Exception("Usage: <testpilot_study> <startdate-YYYY-MM-DD> <enddate-YYYY-MM-DD>")

    # use to collect up each date in the given range
    class MyDateIterator(DateIterator):
       def __init__(self):
          self._list = []
       def get(self):
          return self._list
       def see(self, aTime):
          self._list.append(aTime)

    sdf = SimpleDateFormat(dateformat)
    study = args[0]
    startdate = Calendar.getInstance()
    startdate.setTime(sdf.parse(args[1]))

    enddate = Calendar.getInstance()
    enddate.setTime(sdf.parse(args[2]))

    dates = MyDateIterator()

    DateUtil.iterateByDay(startdate.getTimeInMillis(), enddate.getTimeInMillis(), dates)

    paths = []
    for d in dates.get():
       paths.append(pathformat % (study, sdf.format(Date(d))))

    job.setInputFormatClass(MyInputFormat)
    FileInputFormat.setInputPaths(job, ",".join(paths));
    job.getConfiguration().set("org.mozilla.jydoop.mappertype", "TEXT")
