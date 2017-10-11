package org.neu.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class FlightInfoCompositeKey implements WritableComparable<FlightInfoCompositeKey> {

  private Text year;
  private Text month;
  private Text aaCode;
  private IntWritable recordType; //1-Airport , 2-Airline

  public FlightInfoCompositeKey() {
    this.month = new Text();
    this.year = new Text();
    this.aaCode = new Text();
    this.recordType = new IntWritable();
  }

  public FlightInfoCompositeKey(Text year, Text month, Text aaCode,
      IntWritable recordType) {
    this.year = year;
    this.month = month;
    this.aaCode = aaCode;
    this.recordType = recordType;
  }

  public FlightInfoCompositeKey(String year, String month, String aaCode, int recordType) {
    this(new Text(year), new Text(month), new Text(aaCode), new IntWritable(recordType));
  }

  public static int compare(FlightInfoCompositeKey a, FlightInfoCompositeKey b) {
    return a.compareTo(b);
  }

  public Text getYear() {
    return year;
  }

  public void setYear(Text year) {
    this.year = year;
  }

  public Text getAaCode() {
    return aaCode;
  }

  public void setAaCode(Text aaCode) {
    this.aaCode = aaCode;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    year.write(dataOutput);
    month.write(dataOutput);
    aaCode.write(dataOutput);
    recordType.write(dataOutput);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    year.readFields(dataInput);
    month.readFields(dataInput);
    aaCode.readFields(dataInput);
    recordType.readFields(dataInput);
  }

  @Override
  public int compareTo(FlightInfoCompositeKey key) {
    int code = this.year.compareTo(key.year);
    if (code == 0) {
      code = this.month.compareTo(key.month);
      if (code == 0) {
        code = this.recordType.compareTo(key.recordType);
        if (code == 0) {
          code = this.getAaCode().compareTo(key.getAaCode());
        }
      }
    }
    return code;
  }

  public IntWritable getRecordType() {
    return recordType;
  }

  public void setRecordType(IntWritable recordType) {
    this.recordType = recordType;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof FlightInfoCompositeKey) {
      FlightInfoCompositeKey that = (FlightInfoCompositeKey) obj;
      return this.year.equals(that.year)
          && this.month.equals(that.month)
          && this.recordType.equals(that.recordType)
          && this.aaCode.equals(that.aaCode);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return year.hashCode()
        + month.hashCode()
        + recordType.hashCode()
        + aaCode.hashCode();
  }

  @Override
  public String toString() {
    return year.toString()
        + "," + month.toString()
        + "," + aaCode.toString()
        + "," + recordType.toString();
  }
}
