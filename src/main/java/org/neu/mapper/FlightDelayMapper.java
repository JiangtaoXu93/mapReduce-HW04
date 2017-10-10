package org.neu.mapper;

import static org.neu.util.DataSanity.csvColumnMap;
import static org.neu.util.DataSanity.initCsvColumnMap;
import static org.neu.util.DataSanity.isValidRecord;

import com.opencsv.CSVParser;
import java.io.IOException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.neu.data.FlightCompositeKey;
import org.neu.data.FlightCompositeKey.FlightRecordType;

public class FlightDelayMapper extends Mapper<LongWritable, Text, FlightCompositeKey, IntWritable> {

  private CSVParser csvParser = new CSVParser();

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    initCsvColumnMap();
  }

  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String[] flightRecord = this.csvParser.parseLine(value.toString());
    System.out.println(flightRecord);
    if (flightRecord.length > 0 && isValidRecord(flightRecord)) {
      FlightCompositeKey fKeyAirline = new FlightCompositeKey(
          flightRecord[csvColumnMap.get("month")],
          StringUtils.EMPTY,
          flightRecord[csvColumnMap.get("airlineID")],
          FlightRecordType.RECORD_TYPE_AIRLINE
      );
      FlightCompositeKey fKeyAirport = new FlightCompositeKey(
          flightRecord[csvColumnMap.get("month")],
          flightRecord[csvColumnMap.get("destAirportId")],
          StringUtils.EMPTY,
          FlightRecordType.RECORD_TYPE_AIRPORT
      );

      context.write(fKeyAirport,
          new IntWritable((int)Float.parseFloat(flightRecord[csvColumnMap.get("arrDelayMinutes")])));
      context.write(fKeyAirline,
          new IntWritable((int)Float.parseFloat(flightRecord[csvColumnMap.get("arrDelayMinutes")])));

    }
  }
}
