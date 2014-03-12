import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Job3Reducer extends MapReduceBase
    implements Reducer<Text, Text, Text, Text>{

  public void reduce(Text key, Iterator values,
	      OutputCollector output, Reporter reporter) throws IOException {
		List<String> cache = new ArrayList<String>();
		int sum = 0;
		Text newKey = new Text();
		Text newValue = new Text();
		String keyString = key.toString();
		while (values.hasNext()) {
		    Text value = (Text) values.next();
		    cache.add(value.toString());
		    sum ++;
		}
		for(String valueString:cache) {
		    int i = valueString.indexOf('\t');
		    String doc = valueString.substring(0, i);
		    String word = keyString;
		    String n = valueString.substring(i + 1);
		    newKey.set(word + "," + doc);
		    newValue.set(n + "," + Integer.valueOf(sum).toString());
		    output.collect(newKey, newValue);
		}
  }
}
