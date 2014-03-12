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

public class Job2Reducer extends MapReduceBase
    implements Reducer<Text, Text, Text, Text>{

  public void reduce(Text key, Iterator<Text> values,
	      OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

		List<String> cache = new ArrayList<String>();
		int sum = 0;
		Text newKey = new Text();
		Text newValue = new Text();
		String keyString = key.toString();
		while (values.hasNext()) {
		    Text value = (Text) values.next();
			String valueString = value.toString();
		    int i = valueString.lastIndexOf(",");
		    String n = valueString.substring(i + 1);
		    
		    cache.add(valueString);
		    sum += Integer.valueOf(n);
		}
		for(String valueString:cache) {
		    int i = valueString.indexOf(',');
		    String doc = keyString;
		    String word = valueString.substring(0, i);
		    String n = valueString.substring(i + 1);
		    newKey.set(word + "," + doc);
		    newValue.set(n + "," + Integer.valueOf(sum).toString());
		    output.collect(newKey, newValue);
		}
		
  }
}
