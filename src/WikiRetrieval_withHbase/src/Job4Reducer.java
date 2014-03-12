import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Job4Reducer extends Reducer<Text, Text, ImmutableBytesWritable, Put>
{
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
  	    for (Text value:values) {
	    	String keyString = key.toString();
	    	int i = keyString.indexOf(',');
	    	String word = keyString.substring(0, i);
	    	String doc = keyString.substring(i+1);

    		Put put1 = new Put(Bytes.toBytes(word + "," + doc));
  	        put1.add("value".getBytes(), null, value.toString().getBytes());
    		Put put2 = new Put(Bytes.toBytes(doc + "," + word));
  	        put2.add("value".getBytes(), null, value.toString().getBytes());
  	        
  	        context.write(new ImmutableBytesWritable("word_doc_table_3".getBytes()), put1);
  	        context.write(new ImmutableBytesWritable("doc_word_table_3".getBytes()), put2);
  	      }
    }
}
