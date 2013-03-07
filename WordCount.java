package org.apache.hadoop.examples;


import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.python.core.PyObject;
import org.python.util.PythonInterpreter;

public class WordCount {
  private static PythonInterpreter py = null;
  
  public static void Python(String methodName, Object... args) {
    if (py == null) {
      py = new PythonInterpreter();
      py.exec("import CallJava");
    }
    String argstr="";
    for (int i = 0;i<args.length;i++) {
      Object arg = args[i];
      String var = "arg" + i;
      py.set(var, arg);
      argstr+= var;

      if (i != args.length - 1)
        argstr += ",";

    }
    py.exec("CallJava." + methodName + "("+argstr+")");
  }

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, Text> {
    
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      Python("map", key, value, context);

    }
  }
  
  public static class IntSumReducer 
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      Python("reduce", key, values, context);
    }
  }

  public static void main(String[] args) throws Exception {
    //    Python("map", null, null, null);
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
