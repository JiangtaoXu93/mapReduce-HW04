package org.neu.reducer;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.neu.data.FlightCompositeKey;

/**
 *FlightDelayReducer: combine the number of flight by the same FlightCompositeKey
 *@author jiangtao
 *
 */
public class FlightDelayReducer extends Reducer<FlightCompositeKey, IntWritable, Text, FloatWritable>{
	@Override
	public void reduce(FlightCompositeKey key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int count = 0;
		long totalDelay = 0;
		for (IntWritable value : values) {
      totalDelay +=value.get();
      count++;
		}
		key.setCount(new IntWritable(count));
		context.write(new Text(key.toString()), new FloatWritable(totalDelay));
	}

}
