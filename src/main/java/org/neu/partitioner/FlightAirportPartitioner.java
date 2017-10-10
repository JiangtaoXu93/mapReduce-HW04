package org.neu.partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.neu.data.FlightCompositeKey;

/**
 */
public class FlightAirportPartitioner extends Partitioner<FlightCompositeKey, LongWritable> {

  @Override
  public int getPartition(FlightCompositeKey key, LongWritable longWritable, int numPartitions) {

    return Math.abs((key.getMonth().toString() + key.getAirportCode().toString()).hashCode())
        % numPartitions;
  }
}
