import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Job1Mapper extends MapReduceBase
    implements Mapper<Text, Text, Text, IntWritable> {

public void map(Text key, Text value,
		OutputCollector<Text, IntWritable> output, Reporter reporter)
		throws IOException {
		IntWritable one = new IntWritable(1);
		Text word = new Text();
		String line = value.toString();
	    StringTokenizer tokenizer = new StringTokenizer(line.toLowerCase(), "\t=@#$%^&*\"\\/1234567890_-+| ,()'.!?<>:;[]{}");
		while (tokenizer.hasMoreTokens()) {
			word.set(key.toString().replace(' ','_') + "," + tokenizer.nextToken());
			output.collect(word, one);
		}
	}
}
