/* -*- Mode: Java; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
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
  private static PythonWrapper initPythonWrapper(String pathname, Job job) throws java.io.IOException
  {
    job.getConfiguration().set("org.mozilla.pydoop.scriptname", pathname);
    return new PythonWrapper(pathname);
  }

  static private PythonWrapper getPythonWrapper(Configuration conf) throws java.io.IOException
  {
    String scriptName = conf.get("org.mozilla.pydoop.scriptname");
    if (null == scriptName) {
      throw new java.lang.NullPointerException("scriptName");
    }
    return new PythonWrapper(scriptName);
  }

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
    private PyObject mapfunc;

    public void setup(Context context) throws IOException, InterruptedException
    {
      super.setup(context);
      mapfunc = getPythonWrapper(context.getConfiguration()).getFunction("map");
    }

    public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
      byte[] value_bytes =  value.getValue("data".getBytes(), "json".getBytes());
      Object jparams[] = new Object[] {
          key.toString(),
          new String(value_bytes),
          new ContextWrapper(context)
      };
      mapfunc._jcall(jparams);
    }
  }

  public static class MyCombiner extends Reducer<TypeWritable, TypeWritable, TypeWritable, TypeWritable> {
    private PyObject combinefunc;

    public void setup(Context context) throws IOException, InterruptedException
    {
      super.setup(context);
      combinefunc = getPythonWrapper(context.getConfiguration()).getFunction("combine");
    }

    public void reduce(TypeWritable key, Iterable<TypeWritable> values, Context context) throws IOException, InterruptedException {
      Object jparams[] = new Object[] {
        key.value,
        new WritableIterWrapper(values.iterator()),
        new ContextWrapper(context)
      };
      combinefunc._jcall(jparams);
    }
  }

  public static class MyReducer extends Reducer<TypeWritable, TypeWritable, TypeWritable, TypeWritable>  {
    private PyObject reducefunc;

    public void setup(Context context) throws IOException, InterruptedException
    {
      super.setup(context);
      reducefunc = getPythonWrapper(context.getConfiguration()).getFunction("reduce");
    }

    public void reduce(TypeWritable key, Iterable<TypeWritable> values, Context context) throws IOException, InterruptedException {
        Object jparams[] = new Object[] {
            key.value,
            new WritableIterWrapper(values.iterator()),
            new ContextWrapper(context)
        };
        reducefunc._jcall(jparams);
    }
  }

  private HBaseDriver() {}                               // singleton

  public int run(String[] args) throws Exception {
    if (args.length != 6) {
      System.err.println("Usage: <script_file> <hbase_name> <out_file> yyyyMMdd(start) yyyyMMdd(stop) yyyyMMdd(format)");
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }

    String scriptFile = args[0];
    String tableName = args[1];
    String outPath = args[2];
    String startDate = args[3];
    String stopDate = args[4];
    String dateFormat = args[5];
    
    Path outdir = new Path(outPath);
    Configuration conf = getConf();
    conf.set("mapred.compress.map.output", "true");
    conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
    FileSystem fs = FileSystem.get(conf);

    Job job = new Job(conf, "HBaseDriver");
    job.setJarByClass(HBaseDriver.class);     // class that contains mapper
    try {
      fs.delete(outdir, true);
      System.err.println("Deleted old " + outPath);
    } catch(Exception e) {
    }
    FileOutputFormat.setOutputPath(job, outdir);  // adjust directories as required
    
    SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
    Calendar startCal = Calendar.getInstance();
    startCal.setTime(sdf.parse(startDate));
    Calendar endCal = Calendar.getInstance();
    endCal.setTime(sdf.parse(stopDate));
    List<Pair<String,String>> columns = new ArrayList<Pair<String,String>>(); // family:qualifier
    columns.add(new Pair<String,String>("data", "json"));
    Scan[] scans = MultiScanTableMapReduceUtil.generateBytePrefixScans(startCal, endCal, dateFormat,columns,500, false);

    MultiScanTableMapReduceUtil.initMultiScanTableMapperJob(
                                          tableName,        // input HBase table name
                                          scans,             // Scan instance to control CF and attribute selection
                                          MyMapper.class,   // mapper
                                          TypeWritable.class,             // mapper output key
                                          TypeWritable.class,             // mapper output value
                                          job);

    job.setReducerClass(MyReducer.class);    // reducer class
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(TypeWritable.class);
    job.setOutputValueClass(TypeWritable.class);
    job.setSortComparatorClass(TypeWritable.Comparator.class);

    PythonWrapper module = initPythonWrapper(scriptFile, job);

    if (!job.getConfiguration().get("org.mozilla.pydoop.scriptname").equals(scriptFile)) {
      throw new java.lang.NullPointerException("Whoops");
    }

    boolean maponly = module.getFunction("reduce") == null;
    // set below to 0 to do a map-only job
    job.setNumReduceTasks(maponly ? 0 : 4 );    // at least one, adjust as required

    if (module.getFunction("combine") != null) {
      job.setCombinerClass(MyCombiner.class);
    }

    if (!job.waitForCompletion(true)) {
      return 1;
    }

    // Now read the hadoop files and output a single local file from the results
    FileOutputStream outfs = new FileOutputStream(outPath);
    PrintStream outps = new PrintStream(outfs);

    FileStatus[] files = fs.listStatus(outdir);
    for (FileStatus file : files) {
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
