package org.neu.comparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.neu.data.FlightDelayCompositeKey;


/**
 * FlightSortComparator: a comparator that check the key of year, month, record type, AaCode(airline/airport code)
 * @author Bhanu, Joyal, Jiangtao
 */
public class FlightSortComparator extends WritableComparator {

  protected FlightSortComparator() {
    super(FlightDelayCompositeKey.class, true);
  }

  @Override
  public int compare(WritableComparable a, WritableComparable b) {
    FlightDelayCompositeKey pair1 = (FlightDelayCompositeKey) a;
    FlightDelayCompositeKey pair2 = (FlightDelayCompositeKey) b;
    return FlightDelayCompositeKey.compare(pair1, pair2);

  }

}
