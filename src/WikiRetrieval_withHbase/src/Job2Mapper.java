import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Job2Mapper extends MapReduceBase
    implements Mapper<Text, Text, Text, Text> {

public void map(Text key, Text value,
		OutputCollector<Text, Text> output, Reporter reporter)
		throws IOException {
	Text newKey = new Text();
	Text newValue = new Text();
	String keyString = key.toString();
	String valueString = new String();
    Matcher matcher = Pattern.compile("[0-9]{1,}$").matcher(value.toString());
    if (matcher.find()) valueString = matcher.group();
    else valueString = "Error! Number Format\n";
	int i = keyString.indexOf(",");
	String word = keyString.substring(i+1);
	String doc = keyString.substring(0, i);
	
	//split
	newKey.set(doc);
	newValue.set(word + "," + valueString);
    output.collect(newKey, newValue);
	}
}
