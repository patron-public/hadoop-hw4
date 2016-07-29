package com.training.hadoop.mr;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.training.hadoop.custom.types.CitiesBidCountWritable;
import com.training.hadoop.mr.AvgHighBidCountMapper.ParseErr;

public class AvgHighBidMrTest {

	MapReduceDriver<LongWritable, Text, IntWritable, CitiesBidCountWritable, Text, DoubleWritable> mapReduceDriver;
	List<String> samlpeLines;

	@Before
	public void setup() throws IOException {
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(new AvgHighBidCountMapper(), new AvgHighBidCountReducer());
		String path = new File(getClass().getClassLoader().getResource("sample.txt").getFile()).getAbsolutePath();
		Path wiki_path = Paths.get(path, "");
		samlpeLines = Files.readAllLines(wiki_path, Charset.forName("UTF-8"));
	}

	@Test
	public void testMRValidInput() throws IOException {
		mapReduceDriver.withInput(new LongWritable(0), new Text(samlpeLines.get(0)));
		mapReduceDriver.withInput(new LongWritable(0), new Text(samlpeLines.get(5)));
		mapReduceDriver.withInput(new LongWritable(0), new Text(samlpeLines.get(4)));

		mapReduceDriver.withOutput(new Text(AvgHighBidCountReducer.RESULT_MSG), new DoubleWritable(1.5d));
		mapReduceDriver.withCounter(ParseErr.PARSE_ERR, 0);

		mapReduceDriver.runTest();
	}

	@Test
	public void testMRInvalidInput() throws IOException {

		mapReduceDriver.withInput(new LongWritable(0), new Text(samlpeLines.get(1)));
		mapReduceDriver.withInput(new LongWritable(0), new Text(samlpeLines.get(3)));

		mapReduceDriver.withCounter(ParseErr.PARSE_ERR, 1);
		mapReduceDriver.withCounter(ParseErr.BID_IS_LOW, 1);
		
		mapReduceDriver.runTest();
	}

}
