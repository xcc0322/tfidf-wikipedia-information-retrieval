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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/** An {@link InputFormat} for plain text files.  Files are broken into lines.
 * Either linefeed or carriage-return are used to signal end of line.  Keys are
 * the position in the file, and values are the line of text.. */
public class PageInputFormat extends FileInputFormat<Text, Text> {
	public RecordReader<Text, Text> getRecordReader(InputSplit arg0,
		JobConf arg1, Reporter arg2) throws IOException {
    return new TitleTextRecordReader(arg1, (FileSplit)arg0);
	}

	public static final String SPLIT_MAXSIZE = 
	  "mapreduce.input.fileinputformat.split.maxsize";
	public static final String NUM_INPUT_FILES =
		"mapreduce.input.fileinputformat.numinputfiles";
	private static final double SPLIT_SLOP = 1.1;   // 10% slop
	private static long getMaxSplitSize(JobConf context) {
		return context.getLong(SPLIT_MAXSIZE, Long.MAX_VALUE);
	}
	public InputSplit[] getSplits(JobConf job, int num) throws IOException {
		long minSize = 1;
		long maxSize = getMaxSplitSize(job);

		// generate splits
		List<InputSplit> splits = new ArrayList<InputSplit>();
		FileStatus[] files = listStatus(job);
		for (FileStatus file: files) {
		  Path path = file.getPath();
		  long length = file.getLen();
		  if (length != 0) {
		    BlockLocation[] blkLocations;
		    FileSystem fs = path.getFileSystem(job);
		    blkLocations = fs.getFileBlockLocations(file, 0, length);
		    if (isSplitable(path.getFileSystem(job), path)) {
		      long blockSize = file.getBlockSize();
		      long splitSize = computeSplitSize(blockSize, minSize, maxSize);

		      long bytesRemaining = length;
		      while (((double) bytesRemaining)/splitSize > SPLIT_SLOP) {
		        int blkIndex = getBlockIndex(blkLocations, length-bytesRemaining);
		        splits.add(makeSplit(path, length-bytesRemaining, splitSize,
		                                 blkLocations[blkIndex].getHosts()));
		        bytesRemaining -= splitSize;
		      }

		      if (bytesRemaining != 0) {
		        int blkIndex = getBlockIndex(blkLocations, length-bytesRemaining);
		        splits.add(makeSplit(path, length-bytesRemaining, bytesRemaining,
		                   blkLocations[blkIndex].getHosts()));
		      }
		    } else splits.add(makeSplit(path, 0, length, blkLocations[0].getHosts()));
		  } else splits.add(makeSplit(path, 0, length, new String[0]));
		}
		// Save the number of input files for metrics/loadgen
		job.setLong(NUM_INPUT_FILES, files.length);
		return splits.toArray(new InputSplit[0]);

	}
	protected FileSplit makeSplit(Path file, long start, long length, String[] hosts) {
		 return new FileSplit(file, start, length, hosts);
	}
}

