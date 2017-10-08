package org.neu.job;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.neu.data.FlightCompositeKey;
import org.neu.mapper.FlightCountMapper;
import org.neu.reducer.FlightCountReducer;

public class FlightCountJob extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    Job job = Job.getInstance(getConf(), "FlightCountJob");
    job.setJarByClass(this.getClass());

    FileInputFormat.addInputPath(job, new Path(args[2]));
    FileOutputFormat.setOutputPath(job, new Path(args[3] + "/flightCount"));

    job.setMapperClass(FlightCountMapper.class);
    job.setCombinerClass(FlightCountReducer.class);
    job.setReducerClass(FlightCountReducer.class);
    job.setNumReduceTasks(1);

    job.setOutputKeyClass(FlightCompositeKey.class);
    job.setOutputValueClass(LongWritable.class);

    return job.waitForCompletion(true) ? 0 : 1;

  }
}
