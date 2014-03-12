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

public class Job4Mapper extends Mapper<Text, Text, Text, Text> {

	public static int D = 100000;

	public void map(Text key, Text value, Context context)
		throws IOException, InterruptedException {
			Text newKey = key;
			Text newValue = new Text();
			String valueString = value.toString();
		
			int i2 = valueString.lastIndexOf(",");
			int i1 = valueString.substring(0, i2).lastIndexOf(",");
			int n = Integer.valueOf(valueString.substring(0, i1));
			int N = Integer.valueOf(valueString.substring(i1 + 1, i2));
			int m = Integer.valueOf(valueString.substring(i2 + 1));
			double tfidf = (1.0 * n / N) * Math.log(D / m);
			newValue.set(Double.valueOf(tfidf).toString());
		    context.write(newKey, newValue);
		}
}
