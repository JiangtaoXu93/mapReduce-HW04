package org.neu.combiner;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.neu.data.FlightCountCompositeKey;
import org.neu.data.FlightDataWritable;

/**
 * FlightCountReducer: combine the number of flight by the same FlightCountCompositeKey
 *
 * @author jiangtao
 */
public class FlightCountCombiner extends
    Reducer<FlightCountCompositeKey, FlightDataWritable, FlightCountCompositeKey, FlightDataWritable> {

  @Override
  public void reduce(FlightCountCompositeKey key, Iterable<FlightDataWritable> values,
      Context context)
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