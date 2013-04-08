/* -*- Mode: Java; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
package org.mozilla.jydoop;

import java.io.IOException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.StringTokenizer;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.python.core.Py;
import org.python.core.PyObject;
import org.python.core.PyIterator;
import org.python.core.util.StringUtil;

import org.apache.commons.lang.StringUtils;

public class HBaseDriver extends Configured implements Tool {
  private static PythonWrapper initPythonWrapper(String pathname, Job job) throws IOException
  {
    job.getConfiguration().set("org.mozilla.jydoop.scriptname", pathname);
    return new PythonWrapper(pathname);
  }

  static private PythonWrapper getPythonWrapper(Configuration conf) throws IOException
  {
    String scriptName = conf.get("org.mozilla.jydoop.scriptname");
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
    private PyObject contextobj;

    private static class ColumnID
    {
      public byte[] family;
      public byte[] qualifier;

      public ColumnID(String column)
      {
        String[] splits = column.split(":", 2);
        if (splits.length != 2) {
          throw new AssertionError("Unexpected family:qualifier in org.mozilla.jydoop.hbasecolumns");
        }
        family = splits[0].getBytes();
        qualifier = splits[1].getBytes();
      }
    }

    private ColumnID[] columnlist;

    public void setup(Context context) throws IOException, InterruptedException
    {
      super.setup(context);
      mapfunc = getPythonWrapper(context.getConfiguration()).getFunction("map");
      contextobj = Py.java2py(new ContextWrapper(context));

      // should be family:qualifier[,family:qualifier...]

      String[] columns = context.getConfiguration().get("org.mozilla.jydoop.hbasecolumns").split(",");

      columnlist = new ColumnID[columns.length];
      for (int i = 0; i < columns.length; ++i) {
        columnlist[i] = new ColumnID(columns[i]);
      }
    }

    public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

      // map(k, column1, [column2, ...], context)

      PyObject[] args = new PyObject[2 + columnlist.length];
      args[0] = Py.newString(StringUtil.fromBytes(key.get()));
      args[args.length - 1] = contextobj;
      for (int i = 0; i < columnlist.length; ++i) {
        byte[] vbytes = value.getValue(columnlist[i].family, columnlist[i].qualifier);
        args[i + 1] = vbytes == null ? Py.None : Py.newString(StringUtil.fromBytes(vbytes));
      }
      mapfunc.__call__(args);
    }
  }

  public static class MyCombiner extends Reducer<TypeWritable, TypeWritable, TypeWritable, TypeWritable> {
    private PyObject combinefunc;
    private PyObject contextobj;

    public void setup(Context context) throws IOException, InterruptedException
    {
      super.setup(context);
      combinefunc = getPythonWrapper(context.getConfiguration()).getFunction("combine");
      contextobj = Py.java2py(new ContextWrapper(context));
    }

    public void reduce(TypeWritable key, Iterable<TypeWritable> values, Context context) throws IOException, InterruptedException {
      combinefunc.__call__(key.value, 
                           new WritableIterWrapper(values.iterator()),
                           contextobj);
    }
  }

  public static class MyReducer extends Reducer<TypeWritable, TypeWritable, TypeWritable, TypeWritable>  {
    private PyObject reducefunc;
    private PyObject contextobj;

    public void setup(Context context) throws IOException, InterruptedException
    {
      super.setup(context);
      reducefunc = getPythonWrapper(context.getConfiguration()).getFunction("reduce");
      contextobj = Py.java2py(new ContextWrapper(context));
    }

    public void reduce(TypeWritable key, Iterable<TypeWritable> values, Context context) throws IOException, InterruptedException {
      reducefunc.__call__(key.value,
                          new WritableIterWrapper(values.iterator()),
                          contextobj);
    }
  }

  private HBaseDriver() {}                               // singleton

  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: <script_file> <out_file> <script_options...>");
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }

    String scriptFile = args[0];
    String outPath = args[1];

    Path outdir = new Path(outPath);
    Configuration conf = getConf();
    conf.set("mapred.compress.map.output", "true");
    conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
    FileSystem fs = FileSystem.get(conf);

    String jobname = "HBaseDriver: " + StringUtils.join(args, " ");

    Job job = new Job(conf, jobname);
    job.setJarByClass(HBaseDriver.class);     // class that contains mapper
    try {
      fs.delete(outdir, true);
      System.err.println("Deleted old " + outPath);
    } catch(Exception e) {
    }
    FileOutputFormat.setOutputPath(job, outdir);  // adjust directories as required

    job.setMapOutputKeyClass(TypeWritable.class);
    job.setMapOutputValueClass(TypeWritable.class);
    job.setMapperClass(MyMapper.class);

    job.setReducerClass(MyReducer.class);    // reducer class
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(TypeWritable.class);
    job.setOutputValueClass(TypeWritable.class);
    job.setSortComparatorClass(TypeWritable.Comparator.class);

    PythonWrapper module = initPythonWrapper(scriptFile, job);

    if (!job.getConfiguration().get("org.mozilla.jydoop.scriptname").equals(scriptFile)) {
      throw new java.lang.NullPointerException("Whoops");
    }

    module.getFunction("setupjob")._jcall(new Object[] { job, Arrays.copyOfRange(args, 2, args.length) });

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
