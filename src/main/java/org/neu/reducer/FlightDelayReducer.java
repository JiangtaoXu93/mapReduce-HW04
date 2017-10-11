package org.neu.reducer;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.neu.comparator.FlightDataWritable;
import org.neu.data.FlightCompositeKey;

/**
 * FlightDelayReducer: combine the number of flight by the same FlightCompositeKey
 *
 * @author jiangtao
 */
public class FlightDelayReducer extends
    Reducer<FlightCompositeKey, FlightDataWritable, FlightCompositeKey, FlightDataWritable> {

  @Override
  public void reduce(FlightCompositeKey key, Iterable<FlightDataWritable> values, Context context)
      throws IOException, InterruptedException {

    int count = 0;
    float totalDelay = 0;
    for (FlightDataWritable value : values) {
      totalDelay += value.getDelay().get();
      count += value.getCount().get();
    }
    context.write(key, new FlightDataWritable(totalDelay, count));
  }

}
