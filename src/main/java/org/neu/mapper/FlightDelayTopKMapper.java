package org.neu.mapper;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.neu.comparator.FlightCountComparator;
import org.neu.data.FlightCountCompositeKey;

public class FlightDelayTopKMapper extends
    Mapper<Text, Text, FlightCountCompositeKey, FloatWritable> {

  private final static int YEAR = 0;
  private final static int MONTH = 1;
  private final static int CODE = 2;
  private final static int CODE_TYPE = 3;
  private final static int DELAY = 0;
  private final static int COUNT = 1;

  private static TreeMap<FlightCountCompositeKey, FloatWritable> sortAirportMap = new TreeMap<>(
      new FlightCountComparator());

  private static TreeMap<FlightCountCompositeKey, FloatWritable> sortAirlineMap = new TreeMap<>(
      new FlightCountComparator());

  public void map(Text key, Text value, Context context)
      throws IOException, InterruptedException {

    String[] keys = key.toString().split(",");
    String[] values = value.toString().split(",");

    FlightCountCompositeKey cKey = new FlightCountCompositeKey(keys[YEAR], keys[MONTH], keys[CODE],
        keys[CODE_TYPE],
        values[COUNT]);

    FloatWritable cValue = new FloatWritable(
        Float.parseFloat(values[DELAY]) / Integer.parseInt(values[COUNT]));

    if (1 == cKey.getRecordType().get()) {
      sortAirportMap.put(cKey, cValue);
    } else {
      sortAirlineMap.put(cKey, cValue);
    }

    if (sortAirportMap.size() > 5) {
      sortAirportMap.remove(sortAirportMap.lastKey());
    }
    if (sortAirlineMap.size() > 5) {
      sortAirlineMap.remove(sortAirlineMap.lastKey());
    }
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    for (Map.Entry<FlightCountCompositeKey, FloatWritable> entry : sortAirportMap.entrySet()) {
      context.write(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<FlightCountCompositeKey, FloatWritable> entry : sortAirlineMap.entrySet()) {
      context.write(entry.getKey(), entry.getValue());
    }
  }
}
