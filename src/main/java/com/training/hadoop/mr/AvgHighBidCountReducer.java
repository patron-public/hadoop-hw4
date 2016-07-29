package com.training.hadoop.mr;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.training.hadoop.custom.types.CitiesBidCountWritable;

public class AvgHighBidCountReducer extends Reducer<IntWritable, CitiesBidCountWritable, Text, DoubleWritable> {
	public static final String RESULT_MSG = "Average high bids per city: ";
	
	static enum ResUlt {
		CNT, SZ
	};
	
	@Override
	public void reduce(IntWritable key, Iterable<CitiesBidCountWritable> values, Context context)
			throws IOException, InterruptedException {

		int count = 0;

		Set<Integer> cities = new HashSet<Integer>();
		for (CitiesBidCountWritable value : values) {
			cities.addAll(value.getCities());
			count = count + value.getCount();
		}
		context.getCounter(ResUlt.CNT).setValue(count);
		context.getCounter(ResUlt.SZ).setValue(cities.size());
		// output pair: sum bytes + episodes quantity
		context.write(new Text(RESULT_MSG), new DoubleWritable((double)count / (double)cities.size()));

	}

}
