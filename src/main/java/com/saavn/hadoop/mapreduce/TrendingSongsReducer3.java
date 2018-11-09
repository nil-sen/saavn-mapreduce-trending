package com.saavn.hadoop.mapreduce;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrendingSongsReducer3 extends Reducer<Text, Text, NullWritable, Text> {

	private static final Logger logger = LoggerFactory.getLogger(TrendingSongsReducer3.class);

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
			recordTreeMap.put(Integer.parseInt(fields[0]), new Text(fields[0] + "\t" + fields[1]));
		}catch(NumberFormatException e) {
			logger.error("TrendingSongsMapper.reduce() Integer.parseInt() failed :[" + fields[0] + "|" + fields[1] + "]"
					+ e.getMessage());
			e.printStackTrace();
		}
		if (recordTreeMap.size() > topNCount) {
			recordTreeMap.remove(recordTreeMap.firstKey());
		}
		
	}
	
	@Override
	protected void cleanup(Context context) throws IOException  {
		
		for (Text t : recordTreeMap.descendingMap().values()) {
			try {
				context.write(NullWritable.get(), t);
			} catch (InterruptedException e) {
				logger.error("TrendingSongsReducer.reduce() context.write() failed " + t + e.getMessage());
				e.printStackTrace();
			}
		}
	}
}
