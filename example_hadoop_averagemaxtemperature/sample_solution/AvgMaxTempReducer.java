import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class AvgMaxTempReducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
   
    public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {

    	double count = 0;
        double sum = 0;

        while (values.hasNext()) {
        	sum += values.next().get();
        	count++;
        }

        if (count > 0d) {
        	double result = sum / count;
        	output.collect(key, new DoubleWritable(result)); 
        }

    }
}
