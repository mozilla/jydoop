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
    import java.util.Calendar as Calendar
    import org.apache.hadoop.mapred.FileInputFormat as FileInputFormat
    import org.apache.hadoop.mapred.SequenceFileAsTextInputFormat as SequenceFileAsTextInputFormat

    if len(args) != 3:
        raise Exception("Usage: <testpilot_study> <startdate-YYYY-MM-DD> <enddate-YYYY-MM-DD>")

    study = args[0]
    startdate = args[1]
    enddate = args[2]

    job.setInputFormatClass(SequenceFileAsTextInputFormat.class)

    # TODO: iterate properly
    paths = [pathformat % (study, startdate), pathformat % (study, enddate)]
    
    #    final List<String> paths = new ArrayList<String>();
    #    final String finalLocation = location;
    #    DateUtil.iterateByDay(start.getTimeInMillis(), end.getTimeInMillis(), new DateIterator(){
    #        @Override
    #        public void see(long aTime) {
    #            String aDate = inputPathFormat.format(new Date(aTime));
    #            String path = finalLocation.replace("%DATE%", aDate);
    #            LOG.info("Adding an input location: '" + path + "'");
    #            paths.add(path);
    #        }
    #    });
    #    
    FileInputFormat.setInputPaths(job, ",".join(paths));
