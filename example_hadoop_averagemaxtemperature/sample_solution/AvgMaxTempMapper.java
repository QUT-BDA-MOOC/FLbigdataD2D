import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class AvgMaxTempMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {

    	String line = value.toString();

    	String[] words = line.split(",");

    	try {
	      	double maxtemp = Double.parseDouble(words[5]);
    	  	output.collect(new Text(words[3]), new DoubleWritable(maxtemp));
    	}
    	catch (ArrayIndexOutOfBoundsException e) {
       		// do nothing 
    	}
    	catch (NumberFormatException e) {
        	// do nothing 
    	}
    }
}
