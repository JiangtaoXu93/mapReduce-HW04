package org.neu.job;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.neu.combiner.FlightCountCombiner;
import org.neu.data.FlightCountCompositeKey;
import org.neu.data.FlightDataWritable;
import org.neu.mapper.FlightCountMapper;
import org.neu.reducer.FlightCountReducer;
/**
 * @author Bhanu, Joyal, Jiangtao
 */
public class FlightCountJob extends Configured implements Tool {
  private static String OUTPUT_SEPARATOR = "mapreduce.output.textoutputformat.separator";

  @Override
  public int run(String[] args) throws Exception {

    Job job = Job.getInstance(getConf(), "FlightCountJob");
    job.setJarByClass(this.getClass());
    job.getConfiguration().set(OUTPUT_SEPARATOR, ",");

    FileInputFormat.addInputPath(job, new Path(args[2]));
    FileOutputFormat.setOutputPath(job, new Path(args[3] + "/flightCount"));

    MultipleOutputs.addNamedOutput(job, "flightDelayAirportData", TextOutputFormat.class,
        FlightCountCompositeKey.class, FloatWritable.class);
    MultipleOutputs.addNamedOutput(job, "flightDelayAirlineData", TextOutputFormat.class,
        FlightCountCompositeKey.class, FloatWritable.class);
    MultipleOutputs.addNamedOutput(job, "mostBusyAirportData", TextOutputFormat.class, Text.class,
        Text.class);
    MultipleOutputs.addNamedOutput(job, "mostBusyAirlineData", TextOutputFormat.class, Text.class,
        Text.class);

    LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

    job.setMapperClass(FlightCountMapper.class);
    job.setCombinerClass(FlightCountCombiner.class);
    job.setReducerClass(FlightCountReducer.class);
    job.setNumReduceTasks(1);

    job.setMapOutputKeyClass(FlightCountCompositeKey.class);
    job.setMapOutputValueClass(FlightDataWritable.class);

    job.setOutputKeyClass(FlightCountCompositeKey.class);
    job.setOutputValueClass(FlightDataWritable.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }
}
