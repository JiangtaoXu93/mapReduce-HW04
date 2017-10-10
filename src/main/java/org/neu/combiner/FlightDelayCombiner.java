package org.neu.combiner;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.neu.data.FlightCompositeKey;

/**
 *FlightDelayReducer: combine the number of flight by the same FlightCompositeKey
 *@author jiangtao
 *
 */
public class FlightDelayCombiner extends Reducer<FlightCompositeKey, IntWritable, FlightCompositeKey, IntWritable>{
	@Override
	public void reduce(FlightCompositeKey key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int count = 0;
		//TODO:: Change to Long
		int totalDelay = 0;
		for (IntWritable value : values) {
      totalDelay +=value.get();
      count++;
		}
		key.setCount(new IntWritable(count));
		context.write(key, new IntWritable(totalDelay));
	}

}