package org.neu.partitioner;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.neu.data.FlightInfoCompositeKey;

public class FlightGroupComparator extends WritableComparator {

  public FlightGroupComparator(){
    super(FlightInfoCompositeKey.class, true);
  }

  @Override
  public int compare(WritableComparable w1, WritableComparable w2) {
    FlightInfoCompositeKey k1 = (FlightInfoCompositeKey) w1;
    FlightInfoCompositeKey k2 = (FlightInfoCompositeKey) w2;
    return FlightInfoCompositeKey.compare(k1,k2);
  }

}
