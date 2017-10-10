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
  private IntWritable count;
  private FlightRecordType recordType;

  public FlightCompositeKey() {
    this.month = new Text();
    this.airportCode = new Text();
    this.airlineCode = new Text();
    this.count = new IntWritable();
    this.recordType = FlightRecordType.DEFAULT;
  }

  public FlightCompositeKey(Text month, Text airportCode, Text airline,
      FlightRecordType recordType) {
    this.month = month;
    this.airportCode = airportCode;
    this.airlineCode = airline;
    this.recordType = recordType;
    this.count = new IntWritable(1);
  }

  public FlightCompositeKey(String month, String airportCode, String airline,
      FlightRecordType recordType) {
    this(new Text(month), new Text(airportCode), new Text(airline), recordType);
  }

  public static int groupCompare(FlightCompositeKey a, FlightCompositeKey b) {
    return a.getMonth().compareTo(b.getMonth());
  }

  /*Descending Sort on Count*/
  public static int sortCompare(FlightCompositeKey a, FlightCompositeKey b) {
    int code = a.getMonth().compareTo(b.getMonth());
    if (code != 0) {
      return code;
    }
    return -a.getCount().compareTo(b.getCount());
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
    count.write(dataOutput);
    recordType.
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    month.readFields(dataInput);
    airportCode.readFields(dataInput);
    airlineCode.readFields(dataInput);
    count.readFields(dataInput);
  }

  @Override
  public int compareTo(FlightCompositeKey key) {
    return 0;
  }

  public FlightRecordType getRecordType() {
    return recordType;
  }

  public void setRecordType(FlightRecordType recordType) {
    this.recordType = recordType;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof FlightCompositeKey) {
      FlightCompositeKey that = (FlightCompositeKey) obj;
      if (!recordType.equals(that.recordType) && month.equals(that.month)) {
        return false;
      }
      if (FlightRecordType.RECORD_TYPE_AIRPORT.equals(recordType)) {
        return airportCode.equals(that.getAirportCode());
      }
      if (FlightRecordType.RECORD_TYPE_AIRLINE.equals(recordType)) {
        return airlineCode.equals(that.getAirportCode());
      }
    }
    return false;
  }

  @Override
  public int hashCode() {
    if (FlightRecordType.RECORD_TYPE_AIRPORT.equals(recordType)) {
      return month.hashCode() + 167 * airportCode.hashCode();
    } else {
      return month.hashCode() + 167 * airlineCode.hashCode();
    }
  }

  @Override
  public String toString() {
    return month.toString()
        + " " + airportCode.toString()
        + " " + airlineCode.toString()
        + " " + count.toString();
  }

  public static enum FlightRecordType {
    DEFAULT,
    RECORD_TYPE_AIRPORT,
    RECORD_TYPE_AIRLINE

  }
}
