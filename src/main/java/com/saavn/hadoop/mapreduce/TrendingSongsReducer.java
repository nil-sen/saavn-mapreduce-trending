package com.saavn.hadoop.mapreduce;

import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrendingSongsReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

	private static final Logger logger = LoggerFactory.getLogger(TrendingSongsReducer.class);

	private TreeMap<Integer, Text> recordTreeMap = new TreeMap<Integer, Text>();

	private static final String TOP_N = "top.n";

	private int topNCount;

	@Override
	public void setup(Context context) {
		topNCount = context.getConfiguration().getInt(TOP_N, 0);

	}

	@Override
	public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException {

		for (Text value : values) {
			String[] fields = value.toString().split("\t");

			try {
				recordTreeMap.put(Integer.parseInt(fields[0]), new Text(value));
			}catch(NumberFormatException e) {
				logger.error("TrendingSongsMapper.reduce() Integer.parseInt() failed :[" + fields[0] + "|" + value + "]"
						+ e.getMessage());
				e.printStackTrace();
			}
			if (recordTreeMap.size() > topNCount) {
				recordTreeMap.remove(recordTreeMap.firstKey());
			}
		}

		
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
