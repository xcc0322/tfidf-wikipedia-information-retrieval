import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;

public class Job5Mapper extends Mapper<Text, Text, Text, Text> {

	public static int D = 100000;

	public void map(Text key, Text value, Context context)
		throws IOException, InterruptedException {
			Text newKey = new Text();
			Text newValue = new Text();
			String keyString = key.toString();
			String valueString = value.toString();

			int i = keyString.indexOf(",");
			String doc = keyString.substring(i+1);
			String word = keyString.substring(0, i);
			
			newKey.set(word);
			newValue.set(doc + "," + valueString);
		    context.write(newKey, newValue);
		}
}
