//javac -classpath /usr/lib/hbase/lib/hadoop-core.jar:/usr/lib/hive/lib/commons-cli-1.2.jar:/usr/lib/hbase/hbase-0.90.6-cdh3u4.jar:akela-0.5-SNAPSHOT.jar   HBaseDriver.java  -d out  -Xlint:deprecation  && jar -cvf taras.jar -C out/ . 
//  scan 'telemetry', {LIMIT => 1}
package org.mozilla.pydoop;

import java.io.IOException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.StringTokenizer;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import com.mozilla.util.Pair;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;

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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.hbase.util.Bytes;
import com.mozilla.hadoop.hbase.mapreduce.MultiScanTableMapReduceUtil;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.python.core.PyObject;
import org.python.core.PyIterator;

public class HBaseDriver extends Configured implements Tool {
  static PythonWrapper module = new PythonWrapper("CallJava.py");

  public static class WritableIterWrapper extends PyIterator
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

  public static class ContextWrapper
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
      Object jparams[] = new Object[] {
          key.toString(),
          new String(value_bytes),
          new ContextWrapper(context)
      };
      module.getFunction("map")._jcall(jparams);
    }
  }

  public static class MyReducer extends Reducer<TypeWritable, TypeWritable, TypeWritable, TypeWritable>  {

    public void reduce(TypeWritable key, Iterable<TypeWritable> values, Context context) throws IOException, InterruptedException {
        Object jparams[] = new Object[] {
            key.value,
            new WritableIterWrapper(values.iterator()),
            new ContextWrapper(context)
        };
        module.getFunction("reduce")._jcall(jparams);
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
    FileSystem fs = FileSystem.get(conf);

    Job job = new Job(conf, "HBaseDriver");
    job.setJarByClass(HBaseDriver.class);     // class that contains mapper
    try {
      fs.delete(outdir, true);
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

    MultiScanTableMapReduceUtil.initMultiScanTableMapperJob(
                                          args[0],        // input HBase table name
                                          scans,             // Scan instance to control CF and attribute selection
                                          MyMapper.class,   // mapper
                                          TypeWritable.class,             // mapper output key
                                          TypeWritable.class,             // mapper output value
                                          job);

    job.setReducerClass(MyReducer.class);    // reducer class
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(TypeWritable.class);
    job.setOutputValueClass(TypeWritable.class);

    boolean maponly = module.getFunction("reduce") == null;
    // set below to 0 to do a map-only job
    job.setNumReduceTasks(maponly ? 0 : 4 );    // at least one, adjust as required


    if (!job.waitForCompletion(true)) {
      return 1;
    }

    // Now read the hadoop files and output a single local file from the results
    FileOutputStream outfs = new FileOutputStream(args[1]);
    PrintStream outps = new PrintStream(outfs);

    FileStatus[] files = fs.listStatus(outdir);
    for (FileStatus file : files) {
      System.out.println("Found output file: " + file.getPath() + ", length " + file.getLen());

      if (file.getLen() == 0) {
        continue;
      }
      SequenceFile.Reader r = new SequenceFile.Reader(fs, file.getPath(), conf);

      TypeWritable k = new TypeWritable();
      TypeWritable v = new TypeWritable();
      while (r.next(k, v)) {
        // If this is a map-only job, the keys are usually not valuable so we default
        // to printing only the value.
        // TODO: tab-delimit tuples instead of just .toString on them
        if (!maponly) {
          outps.print(k.toString());
          outps.print('\t');
        }
        outps.print(v.toString());
        outps.println();
      }
    }
    outps.close();
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new HBaseDriver(), args);
    System.exit(res);
  }

}
