package org.neu.mapper;

import static org.neu.util.DataSanity.csvColumnMap;
import static org.neu.util.DataSanity.initCsvColumnMap;
import static org.neu.util.DataSanity.isValidRecord;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.neu.data.FlightCompositeKey;
import org.neu.data.FlightCompositeKey2;

public class FlightDelayMapper2 extends Mapper<LongWritable, Text, FlightCompositeKey2, FloatWritable> {
	private final static int MONTH = 0;
	private final static int CODE = 1;
	private final static int CODE_TYPE = 2;
	private final static int DELAY = 0;
	private final static int COUNT = 0;
	
	


	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String[] tokens = value.toString().split("\t");//get key and value from 1st job output		
		String[] keys = tokens[0].split(",");
		String[] values = tokens[1].split(",");
		
		
		context.write(
				new FlightCompositeKey2(
						keys[MONTH], keys[CODE], values[COUNT],Integer.parseInt(keys[CODE_TYPE])),
				new FloatWritable(
						Float.parseFloat(values[DELAY]) / Integer.parseInt(values[COUNT])));
		
		

		String[] flightRecord = this.csvParser.parseLine(value.toString());
		System.out.println(flightRecord);
		if (flightRecord.length > 0 && isValidRecord(flightRecord)) {
			FlightCompositeKey fKeyAirport = new FlightCompositeKey(
					flightRecord[csvColumnMap.get("month")],
					flightRecord[csvColumnMap.get("destAirportId")],
					StringUtils.EMPTY,
					1
					);
			FlightCompositeKey fKeyAirline = new FlightCompositeKey(
					flightRecord[csvColumnMap.get("month")],
					StringUtils.EMPTY,
					flightRecord[csvColumnMap.get("airlineID")],
					2
					);

			
			context.write(fKeyAirline,
					new IntWritable(
							(int) Float.parseFloat(flightRecord[csvColumnMap.get("arrDelayMinutes")])));

		}
	}
}
