//javac -classpath /usr/lib/hbase/lib/hadoop-core.jar:/usr/lib/hive/lib/commons-cli-1.2.jar:/usr/lib/hbase/hbase-0.90.6-cdh3u4.jar:akela-0.5-SNAPSHOT.jar   HBaseDriver.java  -d out  -Xlint:deprecation  && jar -cvf taras.jar -C out/ . 
//  scan 'telemetry', {LIMIT => 1}
package org.mozilla.pydoop;

import java.io.IOException;
import java.util.StringTokenizer;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import com.mozilla.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import com.mozilla.hadoop.hbase.mapreduce.MultiScanTableMapReduceUtil;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.python.core.PyObject;
import org.python.core.PyIterator;

public class HBaseDriver extends Configured implements Tool {
  static PyObject mapfunc = PythonWrapper.get("map");
  static PyObject reducefunc = PythonWrapper.get("reduce");

  private static class WritableIterWrapper extends PyIterator
  {
    private Iterator<TypeWritable> iter;
    
    public WritableIterWrapper(Iterator<TypeWritable> i)
    {
      iter = i;
    }

    public PyObject __iternext__() {
      if (!iter.hasNext()) {
	return null;
      }
      return iter.next().value;
    }
  }

  private static class ContextWrapper
  {
    private TaskInputOutputContext cx;

    public ContextWrapper(TaskInputOutputContext taskcx)
    {
      cx = taskcx;
    }

    @SuppressWarnings("unchecked")
    public void write(PyObject key, PyObject val) throws IOException, InterruptedException
    {
      cx.write(new TypeWritable(key), new TypeWritable(val));
    }
  }

  public static class MyMapper extends TableMapper<TypeWritable, TypeWritable>  {

    public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
      byte[] value_bytes =  value.getValue("data".getBytes(), "json".getBytes());
      mapfunc._jcall(new Object[] {key.toString(), new String(value_bytes), context});
    }
  }

  public static class MyReducer extends Reducer<TypeWritable, TypeWritable, TypeWritable, TypeWritable>  {

    public void reduce(TypeWritable key, Iterable<TypeWritable> values, Context context) throws IOException, InterruptedException {
      reducefunc._jcall(new Object[] {key.value, new WritableIterWrapper(values.iterator()), context});
    }
  }

  private HBaseDriver() {}                               // singleton

  public int run(String[] args) throws Exception {
    if (args.length != 5) {
      System.err.println("Usage: wordcount <hbase_name> <out_file> yyyyMMdd(start) yyyyMMdd(stop) yyyyMMdd(format)");
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }
    
    Path outdir = new Path(args[1]);
    Configuration conf = getConf();
    conf.set("mapred.compress.map.output", "true");
    conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
    Job job = new Job(conf, "HBaseDriver");
    job.setJarByClass(HBaseDriver.class);     // class that contains mapper
    try {
      FileSystem.get(getConf()).delete(outdir, true);
      System.err.println("Deleted old " + args[1]);
    } catch(Exception e) {
    }
    FileOutputFormat.setOutputPath(job, outdir);  // adjust directories as required
    
    String dateFormat = args[4];//"yyyyMMdd";
    SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
    Calendar startCal = Calendar.getInstance();
    startCal.setTime(sdf.parse(args[2]));
    Calendar endCal = Calendar.getInstance();
    endCal.setTime(sdf.parse(args[3]));
    List<Pair<String,String>> columns = new ArrayList<Pair<String,String>>(); // family:qualifier
    columns.add(new Pair<String,String>("data", "json"));
    Scan[] scans = MultiScanTableMapReduceUtil.generateBytePrefixScans(startCal, endCal, dateFormat,columns,500, false);
    //    MultiScanTableMapReduceUtil.initMultiScanTableMapperJob(TABLE_NAME, scans, TelemetryInvalidCountsMapper.class, Text.class, Text.class, job);

    MultiScanTableMapReduceUtil.initMultiScanTableMapperJob(
                                          args[0],        // input HBase table name
                                          scans,             // Scan instance to control CF and attribute selection
                                          MyMapper.class,   // mapper
                                          Text.class,             // mapper output key
                                          Text.class,             // mapper output value
                                          job);
    
    //    job.setOutputFormatClass(NullOutputFormat.class);   // because we aren't emitting anything from mapper
    job.setReducerClass(MyReducer.class);    // reducer class
    // set below to 0 to do a map-only job
    job.setNumReduceTasks(reducefunc != null ? 1 : 0 );    // at least one, adjust as required


    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new HBaseDriver(), args);
    System.exit(res);
  }

}
