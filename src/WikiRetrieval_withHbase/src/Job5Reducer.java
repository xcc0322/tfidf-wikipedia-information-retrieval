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

class KeyValuePair implements Comparable<KeyValuePair>{
	double y;
	String x;
	KeyValuePair(String x, String y) {
		this.x = new String(x);

		if(y.equals("Infinity")) this.y = 0;
		else this.y = Double.valueOf(y);
	}
	@Override
	public int compareTo(KeyValuePair arg0) {
		if (this.y > arg0.y) return -1;
		if(this.y < arg0.y) return 1;
		return 0;
	}
}

public class Job5Reducer extends Reducer<Text, Text, ImmutableBytesWritable, Put>
{
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

    	String word = key.toString();
        ArrayList<KeyValuePair> list = new ArrayList<KeyValuePair>();
    	for (Text value:values) {
	    	String valueString = value.toString();
			
	    	int i = valueString.lastIndexOf(',');
	    	String doc = valueString.substring(0, i);
	    	String TFIDF = valueString.substring(i+1);

        	list.add(new KeyValuePair(doc, TFIDF));
    	}

        Collections.sort(list);
        for(int i=0; i<Math.min(20, list.size()); i++) {
        	KeyValuePair result = list.get(i);
        	String doc = result.x;
    		Put put1 = new Put(Bytes.toBytes(word + "," + doc));
  	        put1.add("value".getBytes(), null, Double.valueOf(result.y).toString().getBytes());
  	        context.write(new ImmutableBytesWritable("word_doc_table_quick".getBytes()), put1);
        }
    }
}
