import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
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
public class WikiIndex {
  public final static int numberOfJobs = 4;

  public static void main(String[] args) throws IOException {
	long begintime = System.currentTimeMillis();
	JobControl jobctrl = new JobControl("Jobctrl");
    Job jobs[] = new Job[numberOfJobs];
    String dir = "../hadoop/xcc/output_large1/";
    String bufferPath[] = {"../hadoop/xcc/wiki_input_1", "job1", "job2", "job3", "output"};

    for(int i=0; i<numberOfJobs; i++) {
    	String src = bufferPath[i];
    	String des = dir + bufferPath[i + 1];
    	if (i > 0) src = dir + src;
        jobs[i] = createJob(i, src, des);
        if(i > 0) jobs[i].addDependingJob(jobs[i-1]);
        jobctrl.addJob(jobs[i]);
    }
    jobctrl.run();
	long endtime=System.currentTimeMillis();
	long costTime = (endtime - begintime);
	System.out.printf("Total time for sampleinput: %f ms.", (double)costTime/1000);
  }
  private static Job createJob(int id, String inPath, String outPath) throws IOException {
	  switch (id) {
	  	case 0:return createJob1(inPath, outPath);
	  	case 1:return createJob2(inPath, outPath);
	  	case 2:return createJob3(inPath, outPath);
	  	default:return createJob4(inPath, outPath);
	  }
  }
  
  private static Job createJob1(String inPath, String outPath) throws IOException {
	    JobConf conf = new JobConf(WikiIndex.class);
	    System.out.println("create job1");
	    FileInputFormat.setInputPaths(conf, new Path(inPath));
	    FileOutputFormat.setOutputPath(conf, new Path(outPath));

	    conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(IntWritable.class);

	    conf.setInputFormat(Job1InputFormat.class);
	    conf.setMapperClass(Job1Mapper.class);
	    conf.setReducerClass(Job1Reducer.class);
	    return new Job(conf);
  }

  private static Job createJob2(String inPath, String outPath) throws IOException {
    JobConf conf = new JobConf(WikiIndex.class);
    System.out.println("create job2");
    FileInputFormat.setInputPaths(conf, new Path(inPath));
    FileOutputFormat.setOutputPath(conf, new Path(outPath));

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    conf.setInputFormat(KeyValueTextInputFormat.class);
    conf.setMapperClass(Job2Mapper.class);
    conf.setReducerClass(Job2Reducer.class);
    return new Job(conf);
  }
  private static Job createJob3(String inPath, String outPath) throws IOException {
	    JobConf conf = new JobConf(WikiIndex.class);
	    System.out.println("create job3");
	    FileInputFormat.setInputPaths(conf, new Path(inPath));
	    FileOutputFormat.setOutputPath(conf, new Path(outPath));

	    conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(Text.class);

	    conf.setInputFormat(KeyValueTextInputFormat.class);
	    conf.setMapperClass(Job3Mapper.class);
	    conf.setReducerClass(Job3Reducer.class);
	    return new Job(conf);
	  }
  private static Job createJob4(String inPath, String outPath) throws IOException {
	    JobConf conf = new JobConf(WikiIndex.class);
	    System.out.println("create job4");
	    FileInputFormat.setInputPaths(conf, new Path(inPath));
	    FileOutputFormat.setOutputPath(conf, new Path(outPath));

	    conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(Text.class);

	    conf.setInputFormat(KeyValueTextInputFormat.class);
	    conf.setMapperClass(CopyOfJob4Mapper.class);
	    //conf.setReducerClass(Job4Reducer.class);
	    return new Job(conf);
	  }
}
