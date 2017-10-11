package org.neu.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class FlightCompositeKey2 implements WritableComparable<FlightCompositeKey2> {

  private Text month;
  private Text code;
  private IntWritable count;
  private IntWritable recordType; //1-Airport , 2-Airline

  public FlightCompositeKey2() {
    this.month = new Text();
    this.code = new Text();
    this.count = new IntWritable();
    this.recordType = new IntWritable();
  }

  public FlightCompositeKey2(Text month, Text code, IntWritable recordType) {
    this.month = month;
    this.code = code;
    this.recordType = recordType;
    this.count = new IntWritable(1);
  }

  public FlightCompositeKey2(String month, String code, String airline,
      int recordType) {
    this(new Text(month), new Text(code), new IntWritable(recordType));
  }

  public static int groupCompare(FlightCompositeKey2 a, FlightCompositeKey2 b) {
    return a.getMonth().compareTo(b.getMonth());
  }

  /*Descending Sort on Count*/
  public static int sortCompare(FlightCompositeKey2 a, FlightCompositeKey2 b) {
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

  public Text getcode() {
    return code;
  }

  public void setcode(Text code) {
    this.code = code;
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
    code.write(dataOutput);
    count.write(dataOutput);
    recordType.write(dataOutput);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    month.readFields(dataInput);
    code.readFields(dataInput);
    count.readFields(dataInput);
    recordType.readFields(dataInput);
  }

  @Override
  public int compareTo(FlightCompositeKey2 key) {
    return 0;
  }

  public IntWritable getRecordType() {
    return recordType;
  }

  public void setRecordType(IntWritable recordType) {
    this.recordType = recordType;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof FlightCompositeKey2) {
      FlightCompositeKey2 that = (FlightCompositeKey2) obj;
      if (!recordType.equals(that.recordType) && month.equals(that.month)) {
        return false;
      }else 
        return code.equals(that.getcode());    
    }
    return false;
  }

  @Override
  public int hashCode() {
  	StringBuilder sb = new StringBuilder();
  	sb.append(month.toString());
    	sb.append(code.toString());
    return Integer.parseInt(sb.toString());
  }

  @Override
  public String toString() {
    return month.toString()
        + "," + code.toString()
        + "," + count.toString()
        + "," + recordType.toString();
  }
}
