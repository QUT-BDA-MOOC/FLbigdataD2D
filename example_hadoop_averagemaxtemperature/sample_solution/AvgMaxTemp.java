import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class AvgMaxTemp {

  public static void main(String[] args) throws Exception {

    //
    // The expected command-line arguments are the paths containing
    // input and output data. Terminate the job if the number of
    // command-line arguments is not exactly 2.
    //
    if (args.length != 2) {
      System.out.printf(
          "Usage: WordCount <input dir> <output dir>\n");
      System.exit(-1);
    }

    //
    // Instantiate a JobConf object for your job's configuration.  
    //
    // Specify the jar file that contains your driver, mapper, and reducer.
    // Hadoop will transfer this jar file to nodes in your cluster running
    // mapper and reducer tasks.
    //
    JobConf conf = new JobConf(AvgMaxTemp.class);

    //
    // Specify an easily-decipherable name for the job.
    // This job name will appear in reports and logs.
    //
    conf.setJobName("Average Max Temperature");

    conf.setMapperClass(AvgMaxTempMapper.class);
    conf.setReducerClass(AvgMaxTempReducer.class);

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(DoubleWritable.class);   

    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(DoubleWritable.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    //
    // Start the MapReduce job.
    //
    JobClient.runJob(conf);
  }

}
