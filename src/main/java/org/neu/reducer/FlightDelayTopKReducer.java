package org.neu.reducer;

import java.io.IOException;
import java.text.DecimalFormat;
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


  @Override
  public void reduce(FlightCountCompositeKey fKey, Iterable<FloatWritable> values, Context context)
      throws IOException, InterruptedException {
    Text finalVal = getValue(fKey, values);
    context.write(fKey, finalVal);
  }

  private Text getValue(FlightCountCompositeKey fKey, Iterable<FloatWritable> values) {
    DecimalFormat df = new DecimalFormat("##.########");
    float totalDelay = 0;
    for (FloatWritable value : values) {
      totalDelay += value.get();
    }
    return new Text(df.format(totalDelay / fKey.getCount().get()));
  }
}
