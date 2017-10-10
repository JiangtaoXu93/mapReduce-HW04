package org.neu.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class FlightCompositeKey implements WritableComparable<FlightCompositeKey> {

  private Text month;
  private Text airport;
  private Text airline;
  private IntWritable delay;
  private IntWritable count;

  public FlightCompositeKey(Text month, Text airport, Text airline, IntWritable delay) {
    this.month = month;
    this.airport = airport;
    this.airline = airline;
    this.delay = delay;
  }

  public Text getMonth() {
    return month;
  }

  public void setMonth(Text month) {
    this.month = month;
  }

  public Text getAirport() {
    return airport;
  }

  public void setAirport(Text airport) {
    this.airport = airport;
  }

  public Text getAirline() {
    return airline;
  }

  public void setAirline(Text airline) {
    this.airline = airline;
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
    airport.write(dataOutput);
    airline.write(dataOutput);
    delay.write(dataOutput);
    count.write(dataOutput);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    month.readFields(dataInput);
    airport.readFields(dataInput);
    airline.readFields(dataInput);
    delay.readFields(dataInput);
    count.readFields(dataInput);
  }

  @Override
  public int compareTo(FlightCompositeKey o) {
    return 0;
  }
}
