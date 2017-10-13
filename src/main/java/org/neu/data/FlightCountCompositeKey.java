package org.neu.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * @author Bhanu, Joyal, Jiangtao
 */

public class FlightCountCompositeKey implements WritableComparable<FlightCountCompositeKey> {
	// key of the first map output 

	private IntWritable year;
	private IntWritable month;
	private IntWritable aaCode;//airport/airline code
	private Text aaName;//airport/airline name
	private IntWritable recordType; //1-Airport , 2-Airline

	public FlightCountCompositeKey() {
		this.month = new IntWritable();
		this.year = new IntWritable();
		this.aaCode = new IntWritable();
		this.aaName = new Text();
		this.recordType = new IntWritable();
	}

	public FlightCountCompositeKey(IntWritable year, IntWritable month, IntWritable aaCode,
			Text aaName, IntWritable recordType) {
		this.year = year;
		this.month = month;
		this.aaCode = aaCode;
		this.aaName = aaName;
		this.recordType = recordType;
	}

	public FlightCountCompositeKey(String year, String month, String aaCode, String aaName,
			int recordType) {
		this(new IntWritable(Integer.valueOf(year)), new IntWritable(Integer.valueOf(month)),
				new IntWritable(Integer.valueOf(aaCode)), new Text(aaName),
				new IntWritable(recordType));
	}

	public static int compare(FlightCountCompositeKey a, FlightCountCompositeKey b) {
		return a.compareTo(b);
	}


	@Override
	public void write(DataOutput dataOutput) throws IOException {
		year.write(dataOutput);
		month.write(dataOutput);
		aaCode.write(dataOutput);
		aaName.write(dataOutput);
		recordType.write(dataOutput);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		year.readFields(dataInput);
		month.readFields(dataInput);
		aaCode.readFields(dataInput);
		aaName.readFields(dataInput);
		recordType.readFields(dataInput);
	}

	@Override
	public int compareTo(FlightCountCompositeKey key) {
		int code = this.year.compareTo(key.getYear());
		if (code == 0) {
			code = this.month.compareTo(key.getMonth());
			if (code == 0) {
				code = this.recordType.compareTo(key.getRecordType());
				if (code == 0) {
					code = this.getAaCode().compareTo(key.getAaCode());
				}
			}
		}
		return code;
	}


	@Override
	public boolean equals(Object obj) {
		if (obj instanceof FlightCountCompositeKey) {
			FlightCountCompositeKey that = (FlightCountCompositeKey) obj;
			return this.year.equals(that.getYear())
					&& this.month.equals(that.getMonth())
					&& this.recordType.equals(that.getRecordType())
					&& this.aaCode.equals(that.getAaCode());
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
				+ "," + aaName.toString()
				+ "," + recordType.toString();
	}

	public Text getAaName() {
		return aaName;
	}

	public void setAaName(Text aaName) {
		this.aaName = aaName;
	}

	public IntWritable getRecordType() {
		return recordType;
	}

	public void setRecordType(IntWritable recordType) {
		this.recordType = recordType;
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

	public IntWritable getAaCode() {
		return aaCode;
	}

	public void setAaCode(IntWritable aaCode) {
		this.aaCode = aaCode;
	}

}
