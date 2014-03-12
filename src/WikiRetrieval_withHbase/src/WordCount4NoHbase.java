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

public class WordCount4NoHbase {

  public static void main(String[] args) throws IOException {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(WordCount4.class);
	    FileInputFormat.setInputPaths(conf, new Path("../hadoop/xcc/output_huge_2/job3"));
	    FileOutputFormat.setOutputPath(conf, new Path("../hadoop/xcc/output_huge_2/job4"));
	    conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(Text.class);
	    
	    conf.setInputFormat(KeyValueTextInputFormat.class);
	    conf.setMapperClass(Job4MapperNoMapper.class);
	    conf.setReducerClass(Job4ReducerNoReducer.class);
	    client.setConf(conf);
	    try {
	      JobClient.runJob(conf);
	    } catch (Exception e) {
	      e.printStackTrace();
	    }
  }
}
