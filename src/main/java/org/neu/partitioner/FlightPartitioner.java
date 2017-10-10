package org.neu.partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.neu.data.FlightCompositeKey;

/**
 * FlightPartitioner partitions on,
 * <Month>+<AirportCode> or <Month>+<AirlineCode>
 */
public class FlightPartitioner extends Partitioner<FlightCompositeKey, LongWritable> {

  @Override
  public int getPartition(FlightCompositeKey key, LongWritable longWritable, int numPartitions) {

    if (null != key.getAirlineCode()) {
      return Math.abs((key.getMonth().toString() + key.getAirportCode().toString()).hashCode())
          % numPartitions;
    } else {
      return Math.abs((key.getMonth().toString() + key.getAirlineCode().toString()).hashCode())
          % numPartitions;
    }
  }


}
