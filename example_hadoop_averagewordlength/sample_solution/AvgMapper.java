import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

// 
// To define a map function for your MapReduce job, subclass 
// the Mapper class and override the map method.
// The class definition requires four parameters: 
//   The data type of the input key
//   The data type of the input value
//   The data type of the output key (which is the input key type 
//   for the reducer)
//   The data type of the output value (which is the input value 
//   type for the reducer)
//

public class AvgMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    //
    // The map method runs once for each line of text in the input file.
    // The method receives a key of type LongWritable, a value of type
    // Text, and a Context object.
    //
    
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

        //
        // Convert the line, which is received as a Text object,
        // to a String object.
        //
        String line = value.toString();

        for (String word : line.split("\\W+")) {
            if (word.length() > 0) {
                //
                // Obtain the first letter of the word and convert it to lower case
                //
                String letter = word.substring(0, 1).toLowerCase();
                
                //
                // Call the collect method for the OutputCollector object to emit a key
                // and a value from the map method.  The key is the 
                // letter (in lower-case) that the word starts with; the value is the 
                // word's length.
                //
                output.collect(new Text(letter), new IntWritable(word.length()));
            }
        }

    }
}
