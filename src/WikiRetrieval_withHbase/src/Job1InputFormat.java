/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/** An {@link InputFormat} for plain text files.  Files are broken into lines.
 * Either linefeed or carriage-return are used to signal end of line.  Keys are
 * the position in the file, and values are the line of text.. */


class Job1RecordReader implements RecordReader<Text, Text> {
  private LineRecordReader lineReader;
  private LongWritable lineKey;
  private Text lineValue;
  public Job1RecordReader(JobConf job, FileSplit split) throws IOException {
	lineReader = new LineRecordReader(job, split);
    lineKey = lineReader.createKey();
    lineValue = lineReader.createValue();
  }
	public boolean next(Text key, Text value) throws IOException {
	    if (!lineReader.next(lineKey, lineValue)) return false;
	    String input = lineValue.toString();
	    int i = input.indexOf("</title>");
	    if (i<0) return false;
	    String content = input.substring(i + 8);
	    String title = input.substring(7, i);
	    key.set(title);
	    value.set(content);
	    return true;
	}
	public void close() throws IOException {}
	public Text createKey() {
		return new Text();
	}
	public Text createValue() {
		return new Text();
	}
	public long getPos() throws IOException {
		return 0;
	}
	public float getProgress() throws IOException {
		return 0;
	}
}
public class Job1InputFormat extends FileInputFormat<Text, Text> {
	public RecordReader<Text, Text> getRecordReader(InputSplit arg0,
		JobConf arg1, Reporter arg2) throws IOException {
    return new Job1RecordReader(arg1, (FileSplit)arg0);
	}
}

