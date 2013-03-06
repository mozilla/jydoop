//javac -classpath /usr/lib/hbase/lib/hadoop-core.jar:/usr/lib/hive/lib/commons-cli-1.2.jar:/usr/lib/hbase/hbase-0.90.6-cdh3u4.jar:akela-0.5-SNAPSHOT.jar   FoldJSON.java  -d out  -Xlint:deprecation  && jar -cvf taras.jar -C out/ . 
//  scan 'telemetry', {LIMIT => 1}
package taras;

import java.io.IOException;
import java.util.StringTokenizer;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.ArrayList;
import java.util.List;
import com.mozilla.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
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


public class FoldJSON extends Configured implements Tool {

  public static class MyMapper extends TableMapper<Text, IntWritable>  {

    private final IntWritable ONE = new IntWritable(1);
    private Text text = new Text();
    private static int i = 0;
    public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
      if (i < 10) {
        String val = new String(value.getValue(Bytes.toBytes("data"), Bytes.toBytes("json")));
        //      String val = new String(value.getValue(Bytes.toBytes("data"), Bytes.toBytes("timestamp")));
        text.set(""+value.raw()[0].getTimestamp());     // we can only emit Writables...
        context.write(text, ONE);
        i++;
      }
    }
  }

  public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>  {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int i = 0;
      for (IntWritable val : values) {
        i += val.get();
      }
      context.write(key, new IntWritable(i));
    }
  }

  private FoldJSON() {}                               // singleton

  public int run(String[] args) throws Exception {
    if (args.length != 5) {
      System.err.println("Usage: wordcount <hbase_name> <out_file> yyyyMMdd(start) yyyyMMdd(stop) yyyyMMdd(format)");
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }
    
    Path outdir = new Path(args[1]);
    Job job = new Job(getConf(), "FoldJSON");
    job.setJarByClass(FoldJSON.class);     // class that contains mapper
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
                                          IntWritable.class,             // mapper output value
                                          job);
    
    //    job.setOutputFormatClass(NullOutputFormat.class);   // because we aren't emitting anything from mapper
    job.setReducerClass(MyReducer.class);    // reducer class
    job.setNumReduceTasks(2);    // at least one, adjust as required


    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new FoldJSON(), args);
    System.exit(res);
  }

}
