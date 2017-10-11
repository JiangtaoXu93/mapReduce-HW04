package org.neu.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class FlightCompositeKey implements WritableComparable<FlightCompositeKey> {

  private Text year;
  private Text month;
  private Text aaCode;
  private IntWritable recordType; //1-Airport , 2-Airline

  public FlightCompositeKey() {
    this.month = new Text();
    this.year = new Text();
    this.aaCode = new Text();
    this.recordType = new IntWritable();
  }

  public FlightCompositeKey(Text year, Text month, Text aaCode,
      IntWritable recordType) {
    this.year = year;
    this.month = month;
    this.aaCode = aaCode;
    this.recordType = recordType;
  }

  public FlightCompositeKey(String year, String month, String aaCode, int recordType) {
    this(new Text(year), new Text(month), new Text(aaCode), new IntWritable(recordType));
  }

  public static int compare(FlightCompositeKey a, FlightCompositeKey b) {
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
  public int compareTo(FlightCompositeKey key) {
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
    if (obj instanceof FlightCompositeKey) {
      FlightCompositeKey that = (FlightCompositeKey) obj;
      return this.year.equals(that.year)
          && this.month.equals(that.month)
          && this.recordType.equals(that.recordType)
          && this.getAaCode().equals(that.getAaCode());
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
