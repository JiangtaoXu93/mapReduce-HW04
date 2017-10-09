package org.neu.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class MRUtil extends Configured implements Tool {
	public Args args;
	public Configuration conf;
	public String fsroot;
	public Job job;
	public Path inputPath, outputPath;
	public FileSystem fs;
	public String jobName = "MyJob";
	public String jarName = "job.jar";

	/**
	 * This method perform some default initialization common to most apps. All decisions can be overridden in selectMRClasses().
	 * @param args_ Strings passed on the command line
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	public void init(String[] args_) throws IOException, URISyntaxException {
		args = new Args(args_);
		conf = getConf();
		fsroot = args.getOption("-fsroot");
		if (fsroot != null) {
			conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem");
			conf.set("fs.s3.awsAccessKeyId", args.getOption("-awskeyid"));
			conf.set("fs.s3.awsSecretAccessKey", args.getOption("-awskey"));
		}
		job = Job.getInstance(conf, jobName);
		job.setJar(jarName);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		job.setInputFormatClass(WholeFileInputFormat.class);
		FileInputFormat.addInputPath(job, inputPath = new Path(args.getOption("-input")));
		FileOutputFormat.setOutputPath(job, outputPath = new Path(args.getOption("-output")));
		if (fsroot != null) FileSystem.setDefaultUri(conf, new URI(fsroot));
		fs = FileSystem.get(conf);
		if (fs.exists(outputPath)) fs.delete(outputPath, true);
	}

	/**
	 * Run the MR job and then gather the output files, and perform any remaining computations. In the case of "mean" we have to
	 * divide the prices by the number of flights.
	 * 
	 * @arg args the input directory or single input file
	 */
	@Override
	public int run(String[] args_) throws Exception {
		init(args_);
		selectMRClasses(job, args);
		job.waitForCompletion(true);
		cleanUp(job, args, fs, inputPath, outputPath, fsroot);
		Log.close();
		return 0;
	}

	public void cleanUp(Job job, Args args, FileSystem fs, Path in, Path out, String fsroot) throws Exception {}

	public void selectMRClasses(Job job, Args args) {}

	/**
	 * Simple command line argument processing
	 * 
	 * @author Jan Vitek
	 */
	public static class Args {
		public Args(String[] args) {
			this.args = args;
		}
		String[] args;

		public boolean getFlag(String flag) {
			boolean found = false;
			for (int i = 0; i < args.length; i++)
				if (found = args[i].equals(flag)) {
					args[i] = args[0];
					args = Arrays.copyOfRange(args, 1, args.length);
					break;
				}
			return found;
		}

		public String getOption(String opt) {
			opt += "=";
			for (int i = 0; i < args.length; i++)
				if (args[i].startsWith(opt)) {
					String value = args[i].replaceAll(opt, "");
					args[i] = args[0];
					args = Arrays.copyOfRange(args, 1, args.length);
					return value;
				}
			return null;
		}

		int length() {
			return args.length;
		}

		String[] get() {
			return args;
		}

		@Override
		public String toString() {
			String res = "";
			for (String s : args)
				res += s + " ";
			return res;
		}
	}

	public static byte[] writeObject(Object o) throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream os = new ObjectOutputStream(bos);
		os.writeObject(o);
		os.close();
		return bos.toByteArray();
	}

	public static Object readObject(BytesWritable b) throws IOException {
		ByteArrayInputStream bis = new ByteArrayInputStream(b.getBytes());
		ObjectInput in = new ObjectInputStream(bis);
		try {
			return in.readObject();
		} catch (ClassNotFoundException e) {
			throw new Error(e);
		}
	}
}
