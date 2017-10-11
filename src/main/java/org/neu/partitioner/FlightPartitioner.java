package org.neu.partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.neu.data.FlightCompositeKey;

/**
 * FlightPartitioner partitions on,
 * <Month>+<AirportCode> or <Month>+<AirlineCode>
 */
public class FlightPartitioner extends Partitioner<FlightCompositeKey, IntWritable> {

  @Override
  public int getPartition(FlightCompositeKey key, IntWritable intWritable, int numPartitions) {
    return Math.abs(key.getYear().hashCode() * 127) % numPartitions;
  }
}
