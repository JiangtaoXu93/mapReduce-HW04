package org.neu.mapper;

import static org.neu.util.DataSanity.isValidRecord;

import com.opencsv.CSVParser;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.neu.data.FlightCompositeKey;

public class FlightCountMapper extends Mapper<Object, Text, FlightCompositeKey, Object> {

  private CSVParser csvParser = new CSVParser();

  public void map(Object offset, Text value, Context context)
      throws IOException, InterruptedException {
    String[] flightRecord = this.csvParser.parseLine(value.toString());
    if (flightRecord.length > 0 && isValidRecord(flightRecord)) {
      //2.
    }
  }


}
