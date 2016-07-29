package com.training.hadoop.mr;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.training.hadoop.custom.types.CitiesBidCountWritable;

public class AvgHighBidCountMapper extends Mapper<LongWritable, Text, IntWritable, CitiesBidCountWritable> {

	private static final String DELIMITER = "\t";
	private static final IntWritable KEY= new IntWritable(0);

	public static enum ParseErr {
		PARSE_ERR, BID_IS_NULL, BID_IS_LOW
	};

	private static Map<String, String> osMap = new HashMap<String, String>();
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		try {

			String line = value.toString();
			String[] values = line.split(DELIMITER);

			int city = Integer.parseInt(values[7]); 
			int bid = Integer.parseInt(values[19]);


			if (bid == 0) {
				context.getCounter(ParseErr.BID_IS_NULL).increment(1);
				return;
			}

			if (bid < 200) {
				context.getCounter(ParseErr.BID_IS_LOW).increment(1);
				return;
			}

			context.write(KEY, new CitiesBidCountWritable(city, 1));

		} catch (NumberFormatException parseex) {
			context.getCounter(ParseErr.PARSE_ERR).increment(1);
		}

	}
	
	
	
}
