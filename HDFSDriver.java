package taras;


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

public class HDFSDriver {
  static PyObject mapfunc = PythonWrapper.get("map");
  static PyObject reducefunc = PythonWrapper.get("reduce");
  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, Text> {
    
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      mapfunc._jcall(new Object[] {key, value.toString(), context});
    }
  }
  
  public static class IntSumReducer 
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      reducefunc._jcall(new Object[] {key, values, context});
    }
  }

  public static void main(String[] args) throws Exception {
    //    PythonWrapper.call("map", null, null, null);
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "word count");
    /* compression for intermediate data might be a good idea
JobConf conf = new JobConf(getConf(), myApp.class);
...
conf.set("mapred.compress.map.output", "true")
    */
    job.setJarByClass(HDFSDriver.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    job.setNumReduceTasks(reducefunc != null ? 1 : 0 );    // at least one, adjust as required
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
