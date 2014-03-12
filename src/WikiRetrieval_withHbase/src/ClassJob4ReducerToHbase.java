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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableReduce;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ClassJob4ReducerToHbase extends TableReducer<Text, IntWritable, ImmutableBytesWritable>
{
    public void reduce(Text key, Iterator values, Context context) throws IOException, InterruptedException {
  	    while (values.hasNext()) {
	    	Text value = (Text) values.next();
	    	String keyString = key.toString();
	    	int i = keyString.indexOf(',');
	    	String word = keyString.substring(0, i);
	    	String doc = keyString.substring(i+1);

    		Put put1 = new Put(Bytes.toBytes(word + "," + doc));
  	        put1.add("value".getBytes(), null, value.toString().getBytes());
  	        
  	        context.write(null, put1);
  	      }
    }
}
