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

public class WordMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    //
    // The map method runs once for each line of text in the input file.
    // The method receives a key of type LongWritable, a value of type
    // Text, and a Context object.
    //

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

        //
        // Convert the line, which is received as a Text object,
        // to a String object.
        //
        String line = value.toString();

        StringTokenizer tokenizer = new StringTokenizer(line);
           
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            output.collect(word, one);
        } 

        //
        // ALTERNATIVE WAY
        // 

        /*
        //
        // The line.split("\\W+") call uses regular expressions to split the
        // line up by non-word characters.
        // 
        // If you are not familiar with the use of regular expressions in
        // Java code, search the web for "Java Regex Tutorial." 
        //
        for (String word : line.split("\\W+")) {
           
            if (word.length() > 0) {
                //
                // Call the write method on the Context object to emit a key
                // and a value from the map method.
                //
                output.collect(new Text(word), new IntWritable(1));
            }
        }
        */
    }
}
