"""Utilities for querying Firefox Health Report data using jydoop."""

def setupjob(job, args):
    """
    Set up a job to run full table scans for FHR data.
    
    We don't expect any arguments.
    """

    import org.apache.hadoop.hbase.client.Scan as Scan
    import com.mozilla.hadoop.hbase.mapreduce.MultiScanTableMapReduceUtil as MSTMRU

    scan = Scan()
    scan.setCaching(500)
    scan.setCacheBlocks(False)
    scan.addColumn(bytearray('data'), bytearray('json'))

    # FIXME: do it without this multi-scan util
    scans = [scan]
    MSTMRU.initMultiScanTableMapperJob(
        'metrics', scans,
        None, None, None, job)

    # inform HBaseDriver about the columns we expect to receive
    job.getConfiguration().set("org.mozilla.pydoop.hbasecolumns", "data:json");
