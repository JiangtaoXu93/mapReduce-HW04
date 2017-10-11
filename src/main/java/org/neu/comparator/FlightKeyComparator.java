package org.neu.comparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.neu.data.FlightInfoCompositeKey;

public class FlightKeyComparator extends WritableComparator {

  protected FlightKeyComparator() {
    super(FlightInfoCompositeKey.class, true);
  }

  @Override
  public int compare(WritableComparable a, WritableComparable b) {
    FlightInfoCompositeKey pair1 = (FlightInfoCompositeKey) a;
    FlightInfoCompositeKey pair2 = (FlightInfoCompositeKey) b;
    return FlightInfoCompositeKey.compare(pair1,pair2);

  }

}
