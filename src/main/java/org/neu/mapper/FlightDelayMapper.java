package org.neu.mapper;

import static org.neu.util.DataSanity.csvColumnMap;
import static org.neu.util.DataSanity.initCsvColumnMap;
import static org.neu.util.DataSanity.isValidRecord;

import com.opencsv.CSVParser;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.neu.comparator.FlightDataWritable;
import org.neu.data.FlightCompositeKey;

public class FlightDelayMapper extends
    Mapper<LongWritable, Text, FlightCompositeKey, FlightDataWritable> {

  private CSVParser csvParser = new CSVParser();

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    initCsvColumnMap();
  }

  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String[] flightRecord = this.csvParser.parseLine(value.toString());

    if (flightRecord.length > 0 && isValidRecord(flightRecord)) {

      FlightCompositeKey fKeyAirport = new FlightCompositeKey(
          flightRecord[csvColumnMap.get("year")],
          flightRecord[csvColumnMap.get("month")],
          flightRecord[csvColumnMap.get("destAirportId")], 1);
      FlightCompositeKey fKeyAirline = new FlightCompositeKey(
          flightRecord[csvColumnMap.get("year")],
          flightRecord[csvColumnMap.get("month")],
          flightRecord[csvColumnMap.get("airlineID")], 2);

      FlightDataWritable valueAirport = new FlightDataWritable(
          Float.parseFloat(flightRecord[csvColumnMap.get("arrDelayMinutes")]), 1);
      FlightDataWritable valueAirline = new FlightDataWritable(
          Float.parseFloat(flightRecord[csvColumnMap.get("arrDelayMinutes")]), 1);

      context.write(fKeyAirport, valueAirport);
      context.write(fKeyAirline, valueAirline);

    }
  }
}
