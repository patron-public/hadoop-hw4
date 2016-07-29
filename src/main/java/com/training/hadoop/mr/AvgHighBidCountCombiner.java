package com.training.hadoop.mr;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import com.training.hadoop.custom.types.CitiesBidCountWritable;

public class AvgHighBidCountCombiner
		extends Reducer<IntWritable, CitiesBidCountWritable, IntWritable, CitiesBidCountWritable> {

	@Override
	public void reduce(IntWritable key, Iterable<CitiesBidCountWritable> values, Context context)
			throws IOException, InterruptedException {
		Set<Integer> cities = new HashSet<Integer>();

		int count = 0;
		// input value pair: cities + episodes quantity
		for (CitiesBidCountWritable value : values) {
			cities.addAll(value.getCities());
			count = count + value.getCount();
		}

		// output pair: sum bytes + episodes quantity
		context.write(key, new CitiesBidCountWritable(cities, count));

	}

}
