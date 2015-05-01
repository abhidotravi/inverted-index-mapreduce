package com.hadoop.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import java.util.List;
import java.util.ArrayList;

//Sample Test to check the working of Map and Reduce code
public class InvertedIndexTest {
	MapDriver<LongWritable, Text, Text, Text> mapDriver;
	ReduceDriver<Text, Text, Text, Text> reduceDriver;
	
	@Before
	public void setUp() {
		InvertedIndexMapper mapper = new InvertedIndexMapper();
		InvertedIndexReducer reducer = new InvertedIndexReducer();
		mapDriver = MapDriver.newMapDriver();
		mapDriver.setMapper(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}

	@Test
	public void testMapper() {
		mapDriver.withInput(new LongWritable(), new Text(
				"1000,love"));
		mapDriver.addOutput(new Text("love"), new Text("1000"));
		mapDriver.runTest();
	}

	@Test
	public void testReducer() {
		 List<Text> list = new ArrayList<Text>();
		 list.add(new Text("1000"));
		 list.add(new Text("1001"));
		 reduceDriver.setInput(new Text("love"), list);
		 reduceDriver.withOutput(new Text("love"), new Text("1000 1001"));
		 reduceDriver.runTest();
	}
}
