package com.training.hadoop.mr;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import com.training.hadoop.custom.types.CitiesBidCountWritable;
import com.training.hadoop.mr.AvgHighBidCountMapper.ParseErr;

public class AvgHighBidMapperTest {

	MapDriver<LongWritable, Text, IntWritable, CitiesBidCountWritable> mapDriver;
	List<String> samlpeLines;

	@Before
	public void setup() throws IOException {
		mapDriver = MapDriver.newMapDriver(new AvgHighBidCountMapper());
		String path = new File(getClass().getClassLoader().getResource("sample.txt").getFile()).getAbsolutePath();
		Path wiki_path = Paths.get(path, "");
		samlpeLines = Files.readAllLines(wiki_path, Charset.forName("UTF-8"));
	}

	@Test
	public void testMapperPass() throws IOException {
		// bid = 234
		mapDriver.withInput(new LongWritable(0), new Text(samlpeLines.get(0)));
		mapDriver.withOutput(new IntWritable(0), new CitiesBidCountWritable(234, 1));
		mapDriver.runTest();

	}

	@Test
	public void testMapperFailOnParse() throws IOException {
		// bid = "-"
		mapDriver.withInput(new LongWritable(0), new Text(samlpeLines.get(3)));
		mapDriver.withCounter(ParseErr.PARSE_ERR, 1);
		mapDriver.runTest();
	}

	@Test
	public void testMapperLowBid() throws IOException {
		// bid = 199
		mapDriver.withInput(new LongWritable(0), new Text(samlpeLines.get(1)));
		mapDriver.withCounter(ParseErr.BID_IS_LOW, 1);
		mapDriver.runTest();
	}

	// @Test
	// public void testMapperClientCounter() throws IOException {
	//
	// mapDriver.withInput(new LongWritable(0), new Text(samlpeLines.get(0)));
	// mapDriver.withInput(new LongWritable(2), new Text(samlpeLines.get(1)));
	// mapDriver.withInput(new LongWritable(2), new Text(samlpeLines.get(2)));
	// mapDriver.run();
	//
	// assertEquals("Expected 2 counter increment", 2,
	// mapDriver.getCounters().findCounter(ClientName.MOZILLA).getValue());
	// assertEquals("Expected 0 counter increment", 0,
	// mapDriver.getCounters().findCounter(ClientName.OTHER).getValue());
	// }

}
