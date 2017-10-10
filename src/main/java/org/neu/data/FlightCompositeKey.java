package org.neu.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class FlightCompositeKey implements WritableComparable<FlightCompositeKey> {

  private Text month;
  private Text airportCode;
  private Text airlineCode;
  private IntWritable delay;
  private IntWritable count;

  public FlightCompositeKey(Text month, Text airportCode, Text airline, IntWritable delay) {
    this.month = month;
    this.airportCode = airportCode;
    this.airlineCode = airline;
    this.delay = delay;
  }

  public static int groupCompare(FlightCompositeKey a, FlightCompositeKey b) {

    return 0;
  }

  public static int sortCompare(FlightCompositeKey a, FlightCompositeKey b) {
    return 0;
  }

  public Text getMonth() {
    return month;
  }

  public void setMonth(Text month) {
    this.month = month;
  }

  public Text getAirportCode() {
    return airportCode;
  }

  public void setAirportCode(Text airportCode) {
    this.airportCode = airportCode;
  }

  public Text getAirlineCode() {
    return airlineCode;
  }

  public void setAirlineCode(Text airlineCode) {
    this.airlineCode = airlineCode;
  }

  public IntWritable getDelay() {
    return delay;
  }

  public void setDelay(IntWritable delay) {
    this.delay = delay;
  }

  public IntWritable getCount() {
    return count;
  }

  public void setCount(IntWritable count) {
    this.count = count;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    month.write(dataOutput);
    airportCode.write(dataOutput);
    airlineCode.write(dataOutput);
    delay.write(dataOutput);
    count.write(dataOutput);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    month.readFields(dataInput);
    airportCode.readFields(dataInput);
    airlineCode.readFields(dataInput);
    delay.readFields(dataInput);
    count.readFields(dataInput);
  }

  @Override
  public int compareTo(FlightCompositeKey key) {

    return 0;
  }
}
