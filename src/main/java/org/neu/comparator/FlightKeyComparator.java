package org.neu.comparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.neu.data.FlightCompositeKey;

public class FlightKeyComparator extends WritableComparator {

  protected FlightKeyComparator() {
    super(FlightCompositeKey.class, true);
  }

  @Override
  public int compare(WritableComparable a, WritableComparable b) {
    FlightCompositeKey pair1 = (FlightCompositeKey) a;
    FlightCompositeKey pair2 = (FlightCompositeKey) b;
    return FlightCompositeKey.compare(pair1,pair2);

  }

}
