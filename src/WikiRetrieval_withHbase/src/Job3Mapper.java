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

public class Job3Mapper extends MapReduceBase
    implements Mapper<Text, Text, Text, Text> {

public void map(Text key, Text value,
		OutputCollector<Text, Text> output, Reporter reporter)
		throws IOException {
	Text newKey = new Text();
	Text newValue = new Text();
	String keyString = key.toString();
	String valueString = value.toString();

	int i = keyString.indexOf(",");
	String doc = keyString.substring(i+1);
	String word = keyString.substring(0, i);

	newKey.set(word);
	newValue.set(doc + "\t" + valueString);
    output.collect(newKey, newValue);
	}
}
