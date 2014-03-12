import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;



public class WordCount {

  public static void main(String[] args) throws IOException {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(WordCount.class);
	    FileInputFormat.setInputPaths(
	    	conf,
	    	new Path("../hadoop/xcc/wiki_input_1"),
	    	new Path("../hadoop/xcc/wiki_input_2"),
	    	new Path("../hadoop/xcc/wiki_input_3"),
	    	new Path("../hadoop/xcc/wiki_input_4"),
	    	new Path("../hadoop/xcc/wiki_input_5"),
	    	new Path("../hadoop/xcc/wiki_input_6")
	    );
	    FileOutputFormat.setOutputPath(conf, new Path("../hadoop/xcc/output_huge_2/job1"));
	    conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(IntWritable.class);
	    
	    conf.setInputFormat(Job1InputFormat.class);
	    conf.setMapperClass(Job1Mapper.class);
	    conf.setReducerClass(Job1Reducer.class);
	    client.setConf(conf);
	    try {
	      JobClient.runJob(conf);
	    } catch (Exception e) {
	      e.printStackTrace();
	    }
  }
}
