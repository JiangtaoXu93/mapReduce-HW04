package org.neu.reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.neu.comparator.FlightCountComparator;
import org.neu.data.FlightCountCompositeKey;

/**
 * FlightDelayReducer: combine the number of flight by the same FlightInfoCompositeKey
 *
 * @author jiangtao
 */
public class FlightDelayTopKReducer extends
    Reducer<FlightCountCompositeKey, FloatWritable, FlightCountCompositeKey, Text> {

  private static TreeMap<FlightCountCompositeKey, Text> sortAirportMap = new TreeMap<>(
      new FlightCountComparator());

  private static TreeMap<FlightCountCompositeKey, Text> sortAirlineMap = new TreeMap<>(
      new FlightCountComparator());

  private static Map<FlightCountCompositeKey, Text> mp = new HashMap<>();
  @Override
  public void reduce(FlightCountCompositeKey fKey, Iterable<FloatWritable> values, Context context)
      throws IOException, InterruptedException {

    System.out.println(fKey);
    DecimalFormat df = new DecimalFormat("##.######");
    float totalDelay = 0;
    for (FloatWritable value : values) {
      totalDelay += value.get();
    }
    Text finalVal = new Text(df.format(totalDelay / fKey.getCount().get()));

    mp.put(fKey,finalVal);

    if (1 == fKey.getRecordType().get()) {
      sortAirportMap.put(fKey, finalVal);
    } else {
      sortAirlineMap.put(fKey, finalVal);
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
    for (Map.Entry<FlightCountCompositeKey, Text> entry : sortAirportMap.entrySet()) {
      context.write(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<FlightCountCompositeKey, Text> entry : sortAirlineMap.entrySet()) {
      context.write(entry.getKey(), entry.getValue());
    }
  }
}
