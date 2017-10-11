package org.neu.partitioner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.neu.data.FlightCompositeKey;

public class FlightGroupComparator extends WritableComparator {

  public FlightGroupComparator(){
    super(FlightCompositeKey.class, true);
  }

  @Override
  public int compare(WritableComparable w1, WritableComparable w2) {
    FlightCompositeKey k1 = (FlightCompositeKey) w1;
    FlightCompositeKey k2 = (FlightCompositeKey) w2;
    return FlightCompositeKey.compare(k1,k2);
  }

}
