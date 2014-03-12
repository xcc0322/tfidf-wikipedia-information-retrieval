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

public class Job4MapperNoMapper extends MapReduceBase
    implements Mapper<Text, Text, Text, Text> {

public void map(Text key, Text value,
		OutputCollector<Text, Text> output, Reporter reporter)
		throws IOException {
	Text newKey = key;
	Text newValue = new Text();
	String valueString = value.toString();

	int i2 = valueString.lastIndexOf(",");
	int i1 = valueString.substring(0, i2).lastIndexOf(",");
	int n = Integer.valueOf(valueString.substring(0, i1));
	int N = Integer.valueOf(valueString.substring(i1 + 1, i2));
	int m = Integer.valueOf(valueString.substring(i2 + 1));
	int D = 100000;
	double tfidf = (1.0 * n / N) * Math.log(D / m);
	newValue.set(Double.valueOf(tfidf).toString());
    output.collect(newKey, newValue);
	}
}
