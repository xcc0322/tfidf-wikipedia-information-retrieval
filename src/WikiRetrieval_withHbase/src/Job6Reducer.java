import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Job6Reducer extends Reducer<Text, Text, ImmutableBytesWritable, Put>
{
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

    	String doc = key.toString();
        ArrayList<KeyValuePair> list = new ArrayList<KeyValuePair>();
    	for (Text value:values) {
	    	String valueString = value.toString();
			
	    	int i = valueString.lastIndexOf(',');
	    	String word = valueString.substring(0, i);
	    	String TFIDF = valueString.substring(i+1);

        	list.add(new KeyValuePair(word, TFIDF));
    	}

        Collections.sort(list);
        for(int i=0; i<Math.min(20, list.size()); i++) {
        	KeyValuePair result = list.get(i);
        	String word = result.x;
    		Put put1 = new Put(Bytes.toBytes(doc + "," + word));
  	        put1.add("value".getBytes(), null, Double.valueOf(result.y).toString().getBytes());
  	        context.write(new ImmutableBytesWritable("doc_word_table_quick".getBytes()), put1);
        }
    }
}
