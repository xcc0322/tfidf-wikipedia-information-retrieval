import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class WordCount5 {
	  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		    Configuration config = HBaseConfiguration.create();
	        config.set("hbase.zookeeper.property.clientPort", "2181");  
	        config.set("hbase.zookeeper.quorum", "10.141.210.12, 10.141.210.13, 10.141.210.14, 10.141.210.15");  
	        config.set("hbase.master", "10.141.210.11:6000");  
		    Job job = new Job(config,"ExampleSummaryToHBase");
		    job.setJarByClass(WordCount5.class);
		    FileInputFormat.setInputPaths(job, new Path("../hadoop/xcc/output_huge_2/job4"));

	        job.setMapOutputKeyClass(Text.class);
	        job.setMapOutputValueClass(Text.class);
		    
		    job.setInputFormatClass(KeyValueTextInputFormat.class);
		    job.setMapperClass(Job5Mapper.class);
		    job.setReducerClass(Job5Reducer.class);
		    job.setOutputFormatClass(MultiTableOutputFormat.class);
		    job.setNumReduceTasks(1);
		    boolean b = job.waitForCompletion(true);
		    if (!b) {
		      throw new IOException("error with job!");
		    }
	  }
}
