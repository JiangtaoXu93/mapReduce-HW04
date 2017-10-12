package org.neu.mapper;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.neu.data.FlightCountCompositeKey;

public class FlightDelayTopKMapper extends
    Mapper<Text, Text, FlightCountCompositeKey, FloatWritable> {

  private final static int YEAR = 0;
  private final static int MONTH = 1;
  private final static int CODE = 2;
  private final static int CODE_TYPE = 3;
  private final static int DELAY = 0;
  private final static int COUNT = 1;

  public void map(Text key, Text value, Context context)
      throws IOException, InterruptedException {

    String[] keys = key.toString().split(",");
    String[] values = value.toString().split(",");

    FlightCountCompositeKey cKey = new FlightCountCompositeKey(
        keys[YEAR],
        keys[MONTH],
        keys[CODE],
        keys[CODE_TYPE],
        values[COUNT]);

    FloatWritable cValue = new FloatWritable(
        Float.parseFloat(values[DELAY]) / Integer.parseInt(values[COUNT]));
    context.write(cKey, cValue);
  }
}
