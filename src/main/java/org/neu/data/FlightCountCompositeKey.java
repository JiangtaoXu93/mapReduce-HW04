package org.neu.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class FlightCountCompositeKey implements WritableComparable<FlightCountCompositeKey> {

  private IntWritable year;
  private IntWritable month;
  private Text aaCode;
  private Text aaName;
  private IntWritable recordType; //1-Airport , 2-Airline
  private IntWritable count;

  public FlightCountCompositeKey() {
    this.month = new IntWritable();
    this.year = new IntWritable();
    this.aaCode = new Text();
    this.aaName = new Text();
    this.recordType = new IntWritable();
    this.count = new IntWritable();
  }

  public FlightCountCompositeKey(IntWritable year, IntWritable month, Text aaCode,
      Text aaName, IntWritable recordType, IntWritable count) {
    this.year = year;
    this.month = month;
    this.aaCode = aaCode;
    this.aaName = aaName;
    this.recordType = recordType;
    this.count = count;
  }

  public FlightCountCompositeKey(Integer year, Integer month, String aaCode, String aaName,
      Integer recordType,
      Integer count) {
    this(new IntWritable(year),
        new IntWritable(month),
        new Text(aaCode),
        new Text(aaName),
        new IntWritable(recordType),
        new IntWritable(count));
  }

  public FlightCountCompositeKey(String year, String month, String aaCode, String aaName,
      String recordType,
      String count) {
    this(new IntWritable(Integer.valueOf(year)),
        new IntWritable(Integer.valueOf(month)),
        new Text(aaCode),
        new Text(aaName),
        new IntWritable(Integer.valueOf(recordType)),
        new IntWritable(Integer.valueOf(count)));
  }

  public static int grouoCompare(
      FlightCountCompositeKey o1, FlightCountCompositeKey o2) {
    int value = o1.getYear().compareTo(o2.getYear());
    if (value == 0) {
      value = o1.getMonth().compareTo(o2.getMonth());
      if (value == 0) {
        value = o1.getRecordType().compareTo(o2.getRecordType());

      }
    }
    return value;
  }

  public static int compare(
      FlightCountCompositeKey o1, FlightCountCompositeKey o2) {
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
          }
        }
      }
    }
    return value;
  }

  public IntWritable getMonth() {
    return month;
  }

  public void setMonth(IntWritable month) {
    this.month = month;
  }

  public IntWritable getYear() {
    return year;
  }

  public void setYear(IntWritable year) {
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
    aaName.write(dataOutput);
    recordType.write(dataOutput);
    count.write(dataOutput);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    year.readFields(dataInput);
    month.readFields(dataInput);
    aaCode.readFields(dataInput);
    aaName.readFields(dataInput);
    recordType.readFields(dataInput);
    count.readFields(dataInput);
  }

  @Override
  public int compareTo(FlightCountCompositeKey key) {
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
    if (obj instanceof FlightCountCompositeKey) {
      FlightCountCompositeKey that = (FlightCountCompositeKey) obj;
      return this.year.equals(that.year)
          && this.month.equals(that.month)
          && this.recordType.equals(that.recordType)
          && this.aaCode.equals(that.aaCode)
          && this.count.equals(that.count);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return year.hashCode()
        + month.hashCode()
        + recordType.hashCode()
        + aaCode.hashCode()
        + count.hashCode();
  }

  @Override
  public String toString() {
    return year.toString()
        + "," + month.toString()
        + "," + aaCode.toString()
        + "," + aaName.toString()
        + "," + recordType.toString();
  }

  public IntWritable getCount() {
    return count;
  }

  public void setCount(IntWritable count) {
    this.count = count;
  }

  public Text getAaName() {
    return aaName;
  }

  public void setAaName(Text aaName) {
    this.aaName = aaName;
  }
}
