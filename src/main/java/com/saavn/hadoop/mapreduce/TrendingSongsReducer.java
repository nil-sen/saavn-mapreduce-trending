package com.saavn.hadoop.mapreduce;

import java.io.IOException;
import java.util.TreeMap;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrendingSongsReducer extends Reducer<Text, Text, NullWritable, Text> {

	private static final Logger logger = LoggerFactory.getLogger(TrendingSongsReducer.class);
	

	private TreeMap<Integer, Text> recordTreeMap = new TreeMap<Integer, Text>();
	

	private static final String TOP_N = "top.n";
	private static final String TAB = "\t";
	private static final String PIPE = "|";

	private int topNCount;

	@Override
	public void setup(Context context) {
		topNCount = context.getConfiguration().getInt(TOP_N, 0);

	}

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException {

		String[] fields = key.toString().split(TAB);

		try {
			// output count plus song ID			
			//recordTreeMap.put(Integer.parseInt(fields[0]), new Text(fields[0] + TAB + fields[1]));
			// output only song ID as per requirement from upgrad			
			recordTreeMap.put(Integer.parseInt(fields[0]), new Text(fields[1]));
		}catch(NumberFormatException e) {
			logger.error("TrendingSongsReducer.reduce() Integer.parseInt() failed :[" + fields[0] + PIPE + fields[1] + "]"
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
				logger.error("TrendingSongsReducer.cleanup() context.write() failed " + t + e.getMessage());
				e.printStackTrace();
			}
		}
	}
}
