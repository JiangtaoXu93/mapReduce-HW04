package org.neu.job;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.neu.combiner.FlightDelayCombiner;
import org.neu.comparator.FlightDataWritable;
import org.neu.comparator.FlightKeyComparator;
import org.neu.data.FlightCompositeKey;
import org.neu.mapper.FlightDelayMapper;
import org.neu.partitioner.FlightGroupComparator;
import org.neu.partitioner.FlightPartitioner;
import org.neu.reducer.FlightDelayReducer;

public class FlightDelayJob extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    Job job = Job.getInstance(getConf(), "FlightDelayJob");
    job.setJarByClass(this.getClass());

    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2] + "/flightDelay"));

    job.setMapperClass(FlightDelayMapper.class);
    job.setCombinerClass(FlightDelayReducer.class);
    job.setReducerClass(FlightDelayReducer.class);

    //job.setNumReduceTasks(1);

//    job.setPartitionerClass(FlightPartitioner.class);
//    job.setGroupingComparatorClass(FlightGroupComparator.class);
//    job.setSortComparatorClass(FlightKeyComparator.class);

    job.setMapOutputKeyClass(FlightCompositeKey.class);
    job.setMapOutputValueClass(FlightDataWritable.class);

    job.setOutputKeyClass(FlightCompositeKey.class);
    job.setOutputValueClass(FlightDataWritable.class);

    return job.waitForCompletion(true) ? 0 : 1;

  }
}
