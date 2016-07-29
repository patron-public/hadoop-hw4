package com.training.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.training.hadoop.custom.types.CitiesBidCountWritable;


public class AvgHighBidCountDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("Use hdmr <input path> <output path>");
			System.exit(-1);
		}
		
		Job job = Job.getInstance(getConf());
		
		job.setJarByClass(AvgHighBidCountDriver.class);
		job.setJobName("AvgHighBidJob");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapperClass(AvgHighBidCountMapper.class);
		job.setCombinerClass(AvgHighBidCountCombiner.class);
		job.setReducerClass(AvgHighBidCountReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(CitiesBidCountWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new AvgHighBidCountDriver(), args);
		System.exit(res);
	}
}
