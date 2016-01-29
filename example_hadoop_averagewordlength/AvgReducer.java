import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class AvgReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, DoubleWritable> {
   
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {

        //
        // TO DO: implement code
        //




    }
}
