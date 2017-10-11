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
import org.neu.comparator.FlightSortComparator;
import org.neu.data.FlightCompositeKey;
import org.neu.data.FlightCompositeKey2;
import org.neu.mapper.FlightDelayMapper;
import org.neu.mapper.FlightDelayMapper2;
import org.neu.partitioner.FlightGroupComparator;
import org.neu.partitioner.FlightPartitioner;
import org.neu.reducer.FlightDelayReducer;

public class FlightDelayJob2 extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    Job job = Job.getInstance(getConf(), "FlightDelayJob2");
    job.setJarByClass(this.getClass());

    FileInputFormat.addInputPath(job, new Path(args[1] + "1stOutput"));
    FileOutputFormat.setOutputPath(job, new Path(args[2] + "/flightDelay"));

    job.setMapperClass(FlightDelayMapper2.class);
    job.setPartitionerClass(FlightPartitioner2.class);
    job.setCombinerClass(FlightDelayCombiner.class);


    job.setReducerClass(FlightDelayReducer2.class);

    job.setMapOutputKeyClass(FlightCompositeKey2.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);

    return job.waitForCompletion(true) ? 0 : 1;

  }
}
