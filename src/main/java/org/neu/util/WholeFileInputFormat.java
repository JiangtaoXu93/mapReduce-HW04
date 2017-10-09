package org.neu.util;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/* Code from the book more or less verbatim.
 * @author HTDG */
public class WholeFileInputFormat extends FileInputFormat<NullWritable, BytesWritable> {
	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
	}

	@Override
	public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		FiledReader reader = new FiledReader();
		reader.initialize(split, context);
		return reader;
	}

	static private class FiledReader extends RecordReader<NullWritable, BytesWritable> {
		private FileSplit fileSplit;
		private Configuration conf;
		private BytesWritable value = new BytesWritable();
		private boolean processed;

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
			this.fileSplit = (FileSplit) split;
			this.conf = context.getConfiguration();
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (processed) return false;
			byte[] contents = new byte[(int) fileSplit.getLength()];
			Path file = fileSplit.getPath();
			FSDataInputStream in = null;
			try {
				in = file.getFileSystem(conf).open(file);
				IOUtils.readFully(in, contents, 0, contents.length);
				value.set(contents, 0, contents.length);
			} finally {
				IOUtils.closeStream(in);
			}
			processed = true;
			return true;
		}

		@Override
		public NullWritable getCurrentKey() throws IOException, InterruptedException {
			return NullWritable.get();
		}

		@Override
		public BytesWritable getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		@Override
		public float getProgress() throws IOException {
			return processed ? 1 : 0;
		}

		@Override
		public void close() throws IOException {}
	}
}
