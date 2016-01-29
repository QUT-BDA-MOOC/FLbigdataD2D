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

public class AvgReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, DoubleWritable> {
   
    //
    // The reduce method runs once for each key received from
    // the shuffle and sort phase of the MapReduce framework.
    // The method receives a key of type Text, a set of values of type
    // IntWritable, and an OutputCollector object.
    //

    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {

        double sum = 0;
        int count = 0;

        //
        // For each value in the set of values passed to us by the mapper:
        //
        while (values.hasNext()) {
            //
            // Add up the values and increment the count
            //
            sum += values.next().get();
            count++;
        }

        if (count != 0d) {
            //
            // The average length is the sum of the values divided by the count.
            //      
            double result = sum / count;

            //
            // emit a key
            // (the words' starting letter) and 
            // a value (the average length per word starting with this letter) 
            // from the reduce method. 
            //
            output.collect(key, new DoubleWritable(result));      
        }
    }
}
