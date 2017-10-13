package org.neu.data;

public class FlightCodeCountKeyPair implements Comparable<FlightCodeCountKeyPair> {

  private int aaCode;
  private int count;

  public FlightCodeCountKeyPair(int aaCode, int count) {
    super();
    this.aaCode = aaCode;
    this.count = count;
  }

  public int getAaCode() {
    return aaCode;
  }

  public void setAaCode(int aaCode) {
    this.aaCode = aaCode;
  }

  public int getCount() {
    return count;
  }

  public void setCount(int count) {
    this.count = count;
  }

  @Override
  public int compareTo(FlightCodeCountKeyPair o) {
    int value = -(this.count - o.getCount());
    if (value == 0) {
      value = this.aaCode - o.getAaCode();
    }
    return value;
  }
}
