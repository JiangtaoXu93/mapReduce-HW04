package org.neu.comparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.neu.data.FlightCountCompositeKey;
import org.neu.data.FlightInfoCompositeKey;

public class FlightGroupComparator extends WritableComparator {

  public FlightGroupComparator(){
    super(FlightCountCompositeKey.class, true);
  }

  @Override
  public int compare(WritableComparable w1, WritableComparable w2) {
    FlightCountCompositeKey k1 = (FlightCountCompositeKey) w1;
    FlightCountCompositeKey k2 = (FlightCountCompositeKey) w2;
    return FlightCountCompositeKey.grouoCompare(k1,k2);
  }

}
