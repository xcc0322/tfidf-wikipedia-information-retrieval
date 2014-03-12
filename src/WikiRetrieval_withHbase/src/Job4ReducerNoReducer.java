import java.io.IOException;
import java.util.Iterator;

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

public class Job4ReducerNoReducer extends MapReduceBase
    implements Reducer<Text, Text, Text, Text>{

  public void reduce(Text key, Iterator values,
	      OutputCollector output, Reporter reporter) throws IOException {
	    while (values.hasNext()) {
	    	Text value = (Text) values.next();
		    output.collect(key, value);
	      }
  }
}
