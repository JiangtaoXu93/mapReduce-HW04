package org.neu.comparator;

import java.util.Comparator;
import org.neu.data.FlightCountCompositeKey;

public class FlightCountComparator implements Comparator<FlightCountCompositeKey> {

  @Override
  public int compare(FlightCountCompositeKey o1, FlightCountCompositeKey o2) {
    int value = o1.getYear().compareTo(o2.getYear());
    if (value == 0) {
      value = o1.getMonth().compareTo(o2.getMonth());
      if (value == 0) {
        value = o1.getRecordType().compareTo(o2.getRecordType());
        if (value == 0) {
          /*Descending Sort*/
          value = -o1.getCount().compareTo(o2.getCount());
          if (value == 0) {
            value = o1.getAaCode().compareTo(o2.getAaCode());
            System.out.println(value);
          }
        }
      }
    }
    return value;
  }
}
