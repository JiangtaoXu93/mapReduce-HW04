package org.neu.comparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.neu.data.FlightCompositeKey;

public class FlightSortComparator extends WritableComparator {

  protected FlightSortComparator() {
    super(FlightCompositeKey.class, true);
  }

  @Override
  public int compare(WritableComparable a, WritableComparable b) {
    FlightCompositeKey pair1 = (FlightCompositeKey) a;
    FlightCompositeKey pair2 = (FlightCompositeKey) b;
    return FlightCompositeKey.sortCompare(pair1, pair2);

  }

}
