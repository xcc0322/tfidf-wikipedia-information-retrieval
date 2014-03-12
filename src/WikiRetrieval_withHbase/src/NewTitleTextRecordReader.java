import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.LineRecordReader.LineReader;

public class NewTitleTextRecordReader implements RecordReader<Text, Text>  {
  private Text key;
  private Text value;
  private String startTag;
  private String endTag;
  private String startTag_title;
  private String endTag_title;
  private long pos;
  private long start;
  private long end;
  private FSDataInputStream fsin;
  private LineReader in;
  
  public NewTitleTextRecordReader(JobConf job, FileSplit split) throws IOException {
    start = split.getStart();
    pos = split.getStart();
    end = pos + split.getLength();
    startTag = "<text";
    endTag = "</text>";
    startTag_title = "<title";
    endTag_title = "</title>";
    Path file = split.getPath();
    fsin = file.getFileSystem(job).open(file);
    fsin.seek(pos);
    in = new LineReader(fsin, job);
  }

  public boolean next(Text key, Text value) throws IOException {
	  System.out.println("In next()");
      StringBuilder sb = new StringBuilder();
      if (key == null) key = new Text();
      if (value == null) value = new Text();
      int newSize = 0;
      boolean xmlRecordStarted = false;
      Text tmpLine = new Text();
      while (pos < end)
      {
          newSize = in.readLine(tmpLine, Integer.MAX_VALUE, 
              (int)Math.min(Integer.MAX_VALUE, end - pos));
          if (newSize == 0) break;
          if (tmpLine.toString().contains(endTag_title)) {
              String result = tmpLine.toString().trim();
              if(result.startsWith(startTag_title)) result = result.substring(result.indexOf('>')+1);
              if(result.endsWith(endTag_title)) result = result.substring(0, result.length()-endTag_title.length());
              key.set(result);
          }
          if (tmpLine.toString().contains(startTag)) {
        	  sb = new StringBuilder();
              xmlRecordStarted = true;
          }
          if (xmlRecordStarted)
              sb.append(tmpLine.toString());
          if (tmpLine.toString().contains(endTag))
          {
              xmlRecordStarted = false;
              String result = sb.toString().trim();
              if(result.startsWith(startTag)) result = result.substring(result.indexOf('>')+1);
              if(result.endsWith(endTag)) result = result.substring(0, result.length()-endTag.length());
              value.set(result);
              break;
          }
          pos += newSize;
      }
      if (newSize == 0)
      {
          key = null;
          value = null;
          return false;
      }
      else return true;
  } 
  
  public Text getCurrentKey() {
    return key;
  }

  public Text getCurrentValue() {
    return value;
  }

  public float getProgress() throws IOException {
    if (start == end) return 0;
    else return Math.min(1.0f, (pos - start) / (float)(end - start));
  }
  
  public synchronized void close() throws IOException {
    in.close();
  }
  public Text createKey() {
	return new Text();
  }
  public Text createValue() {
	return new Text();
  }
  public long getPos() throws IOException {
	return pos;
  }
}


