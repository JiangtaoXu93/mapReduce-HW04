package org.neu.job;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.neu.combiner.FlightDelayCombiner;
import org.neu.data.FlightDataWritable;
import org.neu.data.FlightInfoCompositeKey;
import org.neu.mapper.FlightDelayMapper;
import org.neu.reducer.FlightDelayReducer;

public class FlightDelayJob extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    Job job = Job.getInstance(getConf(), "FlightDelayJob");
    job.setJarByClass(this.getClass());

    FileInputFormat.addInputPath(job, new Path(args[2]));
    FileOutputFormat.setOutputPath(job, new Path(args[3] + "/flightDelay"));

    MultipleOutputs.addNamedOutput(job, "reducedFlightData", TextOutputFormat.class, FlightInfoCompositeKey.class,
            FlightDataWritable.class);
    MultipleOutputs.addNamedOutput(job, "mostBusyData", TextOutputFormat.class, Text.class,
        Text.class);

    LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

    job.setMapperClass(FlightDelayMapper.class);
    job.setCombinerClass(FlightDelayCombiner.class);
    job.setReducerClass(FlightDelayReducer.class);
    job.setNumReduceTasks(1);

    job.setMapOutputKeyClass(FlightInfoCompositeKey.class);
    job.setMapOutputValueClass(FlightDataWritable.class);

    job.setOutputKeyClass(FlightInfoCompositeKey.class);
    job.setOutputValueClass(FlightDataWritable.class);


    return job.waitForCompletion(true) ? 0 : 1;
  }
}
