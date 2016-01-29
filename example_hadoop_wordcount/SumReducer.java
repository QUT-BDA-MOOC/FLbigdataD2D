import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

// 
// To define a reduce function for your MapReduce job, subclass 
// the Reducer class and override the reduce method.
// The class definition requires four parameters: 
//   The data type of the input key (which is the output key type 
//   from the mapper)
//   The data type of the input value (which is the output value 
//   type from the mapper)
//   The data type of the output key
//   The data type of the output value
//   

public class SumReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
   
    //
    // The reduce method runs once for each key received from
    // the shuffle and sort phase of the MapReduce framework.
    // The method receives a key of type Text, a set of values of type
    // IntWritable, and an OutputCollector object.
    //

    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        
        int sum = 0;

        while (values.hasNext()) {
            sum += values.next().get();
        }

        // 
        // ALTERNATIVE ALTERNATIVE
        //
        /*
		for (IntWritable value : values) {		  
		    //
		    // Add the value to the word count counter for this key.
		    //
	        sum += value.get();
		}
        */

        output.collect(key, new IntWritable(sum));
    }
}
