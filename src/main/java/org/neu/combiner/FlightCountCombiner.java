package org.neu.combiner;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.neu.data.FlightCompositeKey;

/**
 *FlightCountCombiner: combine the number of flight by the same FlightCompositeKey
 *@author jiangtao
 *
 */
public class FlightCountCombiner extends Reducer<FlightCompositeKey, NullWritable, FlightCompositeKey, NullWritable>{
	@Override
	public void reduce(FlightCompositeKey key, Iterable<NullWritable> values, Context context) 
			throws IOException, InterruptedException {
		int count = 0; 
		for (NullWritable value : values) {
			count++;
		}
		key.setCount(new IntWritable(count));
		context.write(key, NullWritable.get()); 
	}

}
