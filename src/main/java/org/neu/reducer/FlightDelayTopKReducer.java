package org.neu.reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.neu.data.FlightDelayCompositeKey;

/**
 * @author Bhanu, Joyal, Jiangtao
 */
public class FlightDelayTopKReducer extends
    Reducer<FlightDelayCompositeKey, FloatWritable, FlightDelayCompositeKey, Text> {


  @Override
  public void reduce(FlightDelayCompositeKey fKey, Iterable<FloatWritable> values, Context context)
      throws IOException, InterruptedException {
    Text finalVal = getValue(fKey, values);
    context.write(fKey, finalVal);
  }

  private Text getValue(FlightDelayCompositeKey fKey, Iterable<FloatWritable> values) {
    DecimalFormat df = new DecimalFormat("##.########");
    float totalDelay = 0;
    for (FloatWritable value : values) {
      totalDelay += value.get();
    }
    return new Text(df.format(totalDelay / fKey.getCount().get()));
  }
}
