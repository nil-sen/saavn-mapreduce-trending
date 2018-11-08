package com.saavn.hadoop.mapreduce;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrendingSongsMapper extends Mapper<Text, Text, NullWritable, Text> {

	private static final Logger logger = LoggerFactory.getLogger(TrendingSongsMapper.class);
	
	private TreeMap<Integer, Text> recordTreeMap = new TreeMap<Integer, Text>();

	private static final String TOP_N = "top.n";

	private int topNCount;

	@Override
	public void setup(Context context) {
		topNCount = context.getConfiguration().getInt(TOP_N, 0);

	}

	@Override
	public void map(Text key, Text value, Context context)  {

		recordTreeMap.put(Integer.parseInt(value.toString()), new Text(value.toString() + "\t" + key.toString()));

		if (recordTreeMap.size() > topNCount) {
			recordTreeMap.remove(recordTreeMap.firstKey());
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException  {
		
		for (Text t : recordTreeMap.values()) {
			try {
				context.write(NullWritable.get(), t);
			} catch (InterruptedException e) {
				logger.error("TrendingSongsMapper.cleanup() context.write() failed " + t + e.getMessage());
				e.printStackTrace();
			}
		}
		
		
		
	}
}
