package org.neu.reducer;

import static org.neu.FlightPerformance.TOP_K_COUNT_CONF_KEY;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.neu.data.FlightCountCompositeKey;

/**
 * FlightDelayReducer: combine the number of flight by the same FlightInfoCompositeKey
 *
 * @author jiangtao
 */
public class FlightDelayTopKReducer extends
    Reducer<FlightCountCompositeKey, FloatWritable, FlightCountCompositeKey, Text> {

  private static int topKCount;
  private Map<Integer, Map<Integer, Integer>> airportRecordCountMap = new HashMap<>();
  private Map<Integer, Map<Integer, Integer>> airlineRecordCountMap = new HashMap<>();

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    topKCount = context.getConfiguration().getInt(TOP_K_COUNT_CONF_KEY, 5);
  }

  @Override
  public void reduce(FlightCountCompositeKey fKey, Iterable<FloatWritable> values, Context context)
      throws IOException, InterruptedException {
    if (checkRecordCount(fKey)) {
      Text finalVal = getValue(fKey, values);
      context.write(fKey, finalVal);
    }
  }

  private Text getValue(FlightCountCompositeKey fKey, Iterable<FloatWritable> values) {
    DecimalFormat df = new DecimalFormat("##.######");
    float totalDelay = 0;
    for (FloatWritable value : values) {
      totalDelay += value.get();
    }
    return new Text(df.format(totalDelay / fKey.getCount().get()));
  }

  private boolean checkRecordCount(FlightCountCompositeKey fKey) {
    if (1 == fKey.getRecordType().get()) {
      int recordCount = airportRecordCountMap
          .computeIfAbsent(fKey.getYear().get(), k -> new HashMap<>())
          .computeIfAbsent(fKey.getMonth().get(), k -> 0);
      airportRecordCountMap.get(fKey.getYear().get()).put(fKey.getMonth().get(), ++recordCount);
      return recordCount <= topKCount;
    } else {
      int recordCount = airlineRecordCountMap
          .computeIfAbsent(fKey.getYear().get(), k -> new HashMap<>())
          .computeIfAbsent(fKey.getMonth().get(), k -> 0);
      airlineRecordCountMap.get(fKey.getYear().get()).put(fKey.getMonth().get(), ++recordCount);
      return recordCount <= topKCount;
    }
  }
}
