package com.saavn.hadoop.mapreduce;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrendingSongsReducer2 extends Reducer<Text, Text, LongWritable, Text> {

	private static final Logger logger = LoggerFactory.getLogger(TrendingSongsReducer2.class);

	private TreeMap<Integer, Text> recordTreeMap = new TreeMap<Integer, Text>();

	private static final String TOP_N = "top.n";

	private int topNCount;

	@Override
	public void setup(Context context) {
		topNCount = context.getConfiguration().getInt(TOP_N, 0);

	}

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException {

		
		String[] fields = key.toString().split("\t");
		
		try {
			context.write(new LongWritable(Long.parseLong(fields[0])), new Text(fields[1]));
		} catch (InterruptedException e) {
			logger.error("TrendingSongsReducer.reduce() context.write() failed " + e.getMessage());
			e.printStackTrace();
		}
		
		
	}
}
