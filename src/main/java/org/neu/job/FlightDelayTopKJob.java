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
import org.neu.comparator.FlightSortComparator;
import org.neu.data.FlightDelayCompositeKey;
import org.neu.mapper.FlightDelayTopKMapper;
import org.neu.reducer.FlightDelayTopKReducer;

public class FlightDelayTopKJob extends Configured implements Tool {

  private static String OUTPUT_SEPARATOR = "mapreduce.output.textoutputformat.separator";

  @Override
  public int run(String[] args) throws Exception {

    Job job = Job.getInstance(getConf(), "FlightDelayTopKJob");
    job.setJarByClass(this.getClass());

    //Normal Input
    FileInputFormat.addInputPath(job, new Path(args[3] + "/flightCount/flightCountData-r-00000"));
    FileOutputFormat.setOutputPath(job, new Path(args[3] + "/flightDelay"));

    //Adding mostBusyData to Distributed Cache
    job.addCacheFile(new Path(args[3] + "/flightCount/mostBusyData-r-00000").toUri());

    job.setMapperClass(FlightDelayTopKMapper.class);
    job.setReducerClass(FlightDelayTopKReducer.class);
    job.setNumReduceTasks(1);

    job.setSortComparatorClass(FlightSortComparator.class);

    job.setInputFormatClass(KeyValueTextInputFormat.class);
    job.getConfiguration().set(OUTPUT_SEPARATOR, ",");
    job.setMapOutputKeyClass(FlightDelayCompositeKey.class);
    job.setMapOutputValueClass(FloatWritable.class);

    job.setOutputKeyClass(FlightDelayCompositeKey.class);
    job.setOutputValueClass(Text.class);


    return job.waitForCompletion(true) ? 0 : 1;
  }
}
