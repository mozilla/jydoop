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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.python.core.Py;
import org.python.core.PyObject;
import org.python.core.PyIterator;
import org.python.core.PyTuple;
import org.python.core.util.StringUtil;

import org.apache.commons.lang.StringUtils;

public class HadoopDriver extends Configured implements Tool {
  public static enum MapperType {
     HBASE, TEXT, JYDOOP;
  }

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
    private Iterator<PythonValue> iter;
    
    public WritableIterWrapper(Iterator<PythonValue> i)
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
      cx.write(new PythonKey(key), new PythonValue(val));
    }
  }

  public static class HBaseMapper extends TableMapper<PythonKey, PythonValue>  {
    private PyObject mapfunc;
    private PyObject contextobj;
    private PythonWrapper pwrapper;

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
      pwrapper = getPythonWrapper(context.getConfiguration());
      mapfunc = pwrapper.getFunction("map");
      contextobj = Py.java2py(new ContextWrapper(context));

      // should be family:qualifier[,family:qualifier...]

      String[] columns = context.getConfiguration().get("org.mozilla.jydoop.hbasecolumns").split(",");

      columnlist = new ColumnID[columns.length];
      for (int i = 0; i < columns.length; ++i) {
        columnlist[i] = new ColumnID(columns[i]);
      }

      PyObject setupfunc = pwrapper.getFunction("mapsetup");
      if (setupfunc != null) {
        setupfunc.__call__(contextobj);
      }
    }

    public void cleanup(Context context)
    {
      PyObject cleanupfunc = pwrapper.getFunction("mapcleanup");
      if (cleanupfunc != null) {
        cleanupfunc.__call__(contextobj);
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

  public static class TextMapper extends Mapper<Text, Text, PythonKey, PythonValue>  {
    private PyObject mapfunc;
    private PyObject contextobj;
    private PythonWrapper pwrapper;

    public void setup(Context context) throws IOException, InterruptedException
    {
      super.setup(context);
      pwrapper = getPythonWrapper(context.getConfiguration());
      mapfunc = pwrapper.getFunction("map");
      contextobj = Py.java2py(new ContextWrapper(context));

      PyObject setupfunc = pwrapper.getFunction("mapsetup");
      if (setupfunc != null) {
        setupfunc.__call__(contextobj);
      }
    }

    public void cleanup(Context context)
    {
      PyObject cleanupfunc = pwrapper.getFunction("mapcleanup");
      if (cleanupfunc != null) {
        cleanupfunc.__call__(contextobj);
      }
    }

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

      // map(key, value, context)

      String[] valueBits = new String(value.getBytes()).split("\t", 2);
      PyObject[] args = new PyObject[3];
      args[0] = Py.newString(key.toString());
      args[1] = Py.newString(value.toString());
      args[2] = contextobj;
      mapfunc.__call__(args);
    }
  }

  public static class JydoopMapper extends Mapper<PythonKey, PythonValue, PythonKey, PythonValue>  {
    private PyObject mapfunc;
    private PyObject contextobj;
    private PythonWrapper pwrapper;

    public void setup(Context context) throws IOException, InterruptedException
    {
      super.setup(context);
      pwrapper = getPythonWrapper(context.getConfiguration());
      mapfunc = pwrapper.getFunction("map");
      contextobj = Py.java2py(new ContextWrapper(context));

      PyObject setupfunc = pwrapper.getFunction("mapsetup");
      if (setupfunc != null) {
        setupfunc.__call__(contextobj);
      }
    }

    public void cleanup(Context context)
    {
      PyObject cleanupfunc = pwrapper.getFunction("mapcleanup");
      if (cleanupfunc != null) {
        cleanupfunc.__call__(contextobj);
      }
    }

    public void map(PythonKey key, PythonValue value, Context context) throws IOException, InterruptedException {

      // map(key, value, context)

      String[] valueBits = new String(value.getBytes()).split("\t", 2);
      PyObject[] args = new PyObject[3];
      args[0] = key.value;
      args[1] = value.value;
      args[2] = contextobj;
      mapfunc.__call__(args);
    }
  }


  public static class MyCombiner extends Reducer<PythonKey, PythonValue, PythonKey, PythonValue> {
    private PyObject combinefunc;
    private PyObject contextobj;

    public void setup(Context context) throws IOException, InterruptedException
    {
      super.setup(context);
      combinefunc = getPythonWrapper(context.getConfiguration()).getFunction("combine");
      contextobj = Py.java2py(new ContextWrapper(context));
    }

    public void reduce(PythonKey key, Iterable<PythonValue> values, Context context) throws IOException, InterruptedException {
      combinefunc.__call__(key.value, 
                           new WritableIterWrapper(values.iterator()),
                           contextobj);
    }
  }

  public static class MyReducer extends Reducer<PythonKey, PythonValue, PythonKey, PythonValue>  {
    private PyObject reducefunc;
    private PyObject contextobj;

    public void setup(Context context) throws IOException, InterruptedException
    {
      super.setup(context);
      reducefunc = getPythonWrapper(context.getConfiguration()).getFunction("reduce");
      contextobj = Py.java2py(new ContextWrapper(context));
    }

    public void reduce(PythonKey key, Iterable<PythonValue> values, Context context) throws IOException, InterruptedException {
      reducefunc.__call__(key.value,
                          new WritableIterWrapper(values.iterator()),
                          contextobj);
    }
  }

  private HadoopDriver() {}                               // singleton

  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: <script_file> <out_file> <script_options...>");
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }

    String scriptFile = args[0];
    String outPath = args[1];

    Path outdir = new Path(outPath);
    final Configuration conf = getConf();
    conf.set("mapred.compress.map.output", "true");
    conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
    final FileSystem fs = FileSystem.get(conf);

    String jobname = "HadoopDriver: " + StringUtils.join(args, " ");

    Job job = new Job(conf, jobname);
    job.setJarByClass(HadoopDriver.class);     // class that contains mapper
    try {
      fs.delete(outdir, true);
    } catch(Exception e) {
    }
    FileOutputFormat.setOutputPath(job, outdir);  // adjust directories as required

    job.setMapOutputKeyClass(PythonKey.class);
    job.setMapOutputValueClass(PythonValue.class);

    job.setReducerClass(MyReducer.class);    // reducer class
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(PythonKey.class);
    job.setOutputValueClass(PythonValue.class);
    job.setSortComparatorClass(PythonKey.Comparator.class);

    PythonWrapper module = initPythonWrapper(scriptFile, job);

    MapperType type = null;

    if (module.getFunction("mappertype") != null) {
       String typeStr = module.getFunction("mappertype")._jcall(new Object[]{});
       type = MapperType.valueOf(typeStr);
    }

    switch(type) {
    case MapperType.HBASE:
       job.setMapperClass(HBaseMapper.class);
       break;
    case MapperType.TEXT:
       job.setMapperClass(TextMapper.class);
       break;
    case MapperType.JYDOOP:
       job.setMapperClass(JydoopMapper.class);
       break;
    default:
       // TODO: Warn?
       // Default to HBaseMapper
       job.setMapperClass(HBaseMapper.class);
       break;
    }

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

    // Now read the hadoop files and call the output function

    final FileStatus[] files = fs.listStatus(outdir);

    class KeyValueIterator extends PyIterator
    {
      int index;
      SequenceFile.Reader r;

      public KeyValueIterator() {
        index = 0;
      }

      public PyObject __iternext__()
      {
        PythonKey k = new PythonKey();
        PythonValue v = new PythonValue();
        try {
          for ( ; index < files.length; r = null, ++index) {
            if (r == null) {
              if (files[index].getLen() == 0) {
                continue;
              }
              r = new SequenceFile.Reader(fs, files[index].getPath(), conf);
            }
            if (r.next(k, v)) {
              return new PyTuple(k.value, v.value);
            }
          }
        }
        catch (IOException e) {
          throw Py.IOError(e);
        }
        return null;
      }
    }

    PyObject outputfunc = module.getFunction("output");
    if (outputfunc == null) {
      if (maponly) {
        outputfunc = org.python.core.imp.load("jydoop").__getattr__("outputWithoutKey");
      } else {
        outputfunc = org.python.core.imp.load("jydoop").__getattr__("outputWithKey");
      }
    }
    outputfunc.__call__(Py.newString(outPath), new KeyValueIterator());

    // If we got here, the temporary files are irrelevant. Delete them.
    fs.delete(outdir, true);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new HadoopDriver(), args);
    System.exit(res);
  }

}
