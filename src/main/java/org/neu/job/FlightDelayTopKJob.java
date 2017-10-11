package org.neu.job;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.neu.data.FlightCountCompositeKey;
import org.neu.mapper.FlightDelayTopKMapper;
import org.neu.reducer.FlightDelayTopKReducer;

public class FlightDelayTopKJob extends Configured implements Tool {

  public static String SEPERATOR = "mapreduce.output.textoutputformat.separator";

  @Override
  public int run(String[] args) throws Exception {

    Job job = Job.getInstance(getConf(), "FlightDelayTopKJob");
    job.setJarByClass(this.getClass());

    FileInputFormat.addInputPath(job, new Path(args[2] + "/flightDelay"));
    FileOutputFormat.setOutputPath(job, new Path(args[2] + "/flightDelaySorted"));

    job.setMapperClass(FlightDelayTopKMapper.class);
    job.setReducerClass(FlightDelayTopKReducer.class);

    //    job.setPartitionerClass(FlightPartitioner.class);

    job.setInputFormatClass(KeyValueTextInputFormat.class);
    job.getConfiguration().set(SEPERATOR, ",");
    job.setMapOutputKeyClass(FlightCountCompositeKey.class);
    job.setMapOutputValueClass(FloatWritable.class);

    job.setOutputKeyClass(FlightCountCompositeKey.class);
    job.setOutputValueClass(Text.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }
}
