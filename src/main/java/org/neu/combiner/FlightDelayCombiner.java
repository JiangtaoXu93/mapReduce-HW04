package org.neu.combiner;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.neu.data.FlightInfoCompositeKey;

/**
 * FlightDelayReducer: combine the number of flight by the same FlightInfoCompositeKey
 *
 * @author jiangtao
 */
public class FlightDelayCombiner extends
    Reducer<FlightInfoCompositeKey, IntWritable, FlightInfoCompositeKey, IntWritable> {

  @Override
    public void reduce(FlightInfoCompositeKey key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
    int count = 0;
    int totalDelay = 0;
    for (IntWritable value : values) {
      totalDelay += value.get();
      count++;
    }
//    key.setCount(new IntWritable(count));
    context.write(key, new IntWritable(totalDelay));
  }

}
