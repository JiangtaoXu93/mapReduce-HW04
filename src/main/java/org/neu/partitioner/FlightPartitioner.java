package org.neu.partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.neu.data.FlightInfoCompositeKey;

/**
 * FlightPartitioner partitions on,
 * <Month>+<AirportCode> or <Month>+<AirlineCode>
 */
public class FlightPartitioner extends Partitioner<FlightInfoCompositeKey, IntWritable> {

  @Override
  public int getPartition(FlightInfoCompositeKey key, IntWritable intWritable, int numPartitions) {
    return Math.abs(key.getYear().hashCode() * 127) % numPartitions;
  }
}
