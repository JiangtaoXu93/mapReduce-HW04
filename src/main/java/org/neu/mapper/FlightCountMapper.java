package org.neu.mapper;

import static org.neu.util.DataSanity.csvColumnMap;
import static org.neu.util.DataSanity.initCsvColumnMap;
import static org.neu.util.DataSanity.isValidRecord;

import com.opencsv.CSVParser;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.neu.data.FlightCountCompositeKey;
import org.neu.data.FlightDataWritable;
/**
 * @author Bhanu, Joyal, Jiangtao
 */
public class FlightCountMapper extends
    Mapper<LongWritable, Text, FlightCountCompositeKey, FlightDataWritable> {

  private static CSVParser csvParser = new CSVParser();

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    initCsvColumnMap();
  }

  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String[] flightRecord = csvParser.parseLine(value.toString());

    if (flightRecord.length > 0 && isValidRecord(flightRecord)) {

      Float delayMinutes = getDelayMinutes(flightRecord);
      FlightCountCompositeKey fKeyAirport = new FlightCountCompositeKey(
          flightRecord[csvColumnMap.get("year")],
          flightRecord[csvColumnMap.get("month")],
          flightRecord[csvColumnMap.get("destAirportId")],
          flightRecord[csvColumnMap.get("destination")], 1);
      FlightCountCompositeKey fKeyAirline = new FlightCountCompositeKey(
          flightRecord[csvColumnMap.get("year")],
          flightRecord[csvColumnMap.get("month")],
          flightRecord[csvColumnMap.get("airlineID")],
          flightRecord[csvColumnMap.get("uniqueCarrier")], 2);

      context.write(fKeyAirport, new FlightDataWritable(delayMinutes, 1));
      context.write(fKeyAirline, new FlightDataWritable(delayMinutes, 1));

    }
  }

  private Float getDelayMinutes(String[] flightRecord) {
    Float delay;
    if (Integer.parseInt(flightRecord[csvColumnMap.get("cancelled")]) == 1) {
      delay = 4F;
    } else {
      delay = Float.parseFloat(flightRecord[csvColumnMap.get("arrDelayMinutes")]) /
          Float.parseFloat(flightRecord[csvColumnMap.get("crsElapsedTime")]);
    }
    return delay;
  }
}
