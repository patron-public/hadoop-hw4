package com.training.hadoop.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.training.hadoop.custom.types.CitiesBidCountWritable;

public class AvgHighBidReducerTest {

	ReduceDriver<IntWritable, CitiesBidCountWritable, Text, DoubleWritable> reduceDriver;
	
	@Before
	public void setup() throws IOException {
		reduceDriver = ReduceDriver.newReduceDriver(new AvgHighBidCountReducer());
	}

	@Test
	public void testReducerMath() throws IOException {
		
		List<CitiesBidCountWritable> values = new ArrayList<CitiesBidCountWritable>();

		values.add(new CitiesBidCountWritable(1, 1));
		values.add(new CitiesBidCountWritable(2, 1));
		values.add(new CitiesBidCountWritable(2 ,4));
		values.add(new CitiesBidCountWritable(2, 1));
		values.add(new CitiesBidCountWritable(3, 1));
		values.add(new CitiesBidCountWritable(4, 2));
		
		reduceDriver.withInput(new IntWritable(0), values);

		reduceDriver.withOutput(new Text(AvgHighBidCountReducer.RESULT_MSG), new DoubleWritable(2.5d));

		reduceDriver.runTest();

	}

}
