import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

// 
// MapReduce jobs are typically implemented by using a driver class.
// The purpose of a driver class is to set up the configuration for the
// MapReduce job and to run the job.
//
// Typical requirements for a driver class include configuring the input
// and output data formats, configuring the map and reduce classes,
// and specifying intermediate data formats.
// 
// The following is the code for the driver class:
//

public class WordCount {

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
    JobConf conf = new JobConf(WordCount.class);

    //
    // Specify an easily-decipherable name for the job.
    // This job name will appear in reports and logs.
    //
    conf.setJobName("WordCount");

    //
    // Specify the mapper and reducer classes.
    //
    conf.setMapperClass(WordMapper.class);
    conf.setReducerClass(SumReducer.class);

    //
    // For the word count application, the input file and output 
    // files are in text format - the default format.
    // 
    // In text format files, each record is a line delineated by a 
    // by a line terminator.
    // 
    // When you use other input formats, you must call the 
    // SetInputFormatClass method. When you use other 
    // output formats, you must call the setOutputFormatClass method.
    //
      
    //
    // For the word count application, the mapper's output keys and
    // values have the same data types as the reducer's output keys 
    // and values: Text and IntWritable.
    // 
    // When they are not the same data types, you must call the 
    // setMapOutputKeyClass and setMapOutputValueClass 
    // methods.
    //

    //
    // Specify the job's output key and value classes.
    //
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);   

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
