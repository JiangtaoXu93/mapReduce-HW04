package org.neu.reducer;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.neu.data.FlightDataWritable;
import org.neu.data.FlightInfoCompositeKey;

/**
 * FlightDelayReducer: combine the number of flight by the same FlightInfoCompositeKey
 *
 * @author jiangtao
 */
public class FlightDelayReducer extends
    Reducer<FlightInfoCompositeKey, FlightDataWritable, FlightInfoCompositeKey, FlightDataWritable> {

  @Override
  public void reduce(FlightInfoCompositeKey key, Iterable<FlightDataWritable> values, Context context)
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
