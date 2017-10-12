package org.neu.comparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.neu.data.FlightCountCompositeKey;

public class FlightSortComparator extends WritableComparator {

  protected FlightSortComparator() {
    super(FlightCountCompositeKey.class, true);
  }

  @Override
  public int compare(WritableComparable a, WritableComparable b) {
    FlightCountCompositeKey pair1 = (FlightCountCompositeKey) a;
    FlightCountCompositeKey pair2 = (FlightCountCompositeKey) b;
    return FlightCountCompositeKey.compare(pair1, pair2);

  }

}
