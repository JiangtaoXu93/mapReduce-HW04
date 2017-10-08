package org.neu.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class FlightCompositeKey implements WritableComparable<FlightCompositeKey> {


  private Text month;
  private Text entity;

  @Override
  public void write(DataOutput dataOutput) throws IOException {

  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {

  }

  @Override
  public int compareTo(FlightCompositeKey o) {
    return 0;
  }
}
