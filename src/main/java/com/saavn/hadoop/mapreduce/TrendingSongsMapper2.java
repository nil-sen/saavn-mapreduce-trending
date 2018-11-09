package com.saavn.hadoop.mapreduce;

import java.io.IOException;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrendingSongsMapper2 extends Mapper<Text, Text, Text, Text> {

	private static final Logger logger = LoggerFactory.getLogger(TrendingSongsMapper2.class);
	
	private TreeMap<Integer, Text> recordTreeMap = new TreeMap<Integer, Text>();

	private static final String TOP_N = "top.n";

	private int topNCount;

	@Override
	public void setup(Context context) {
		topNCount = context.getConfiguration().getInt(TOP_N, 0);

	}

	@Override
	public void map(Text key, Text value, Context context) throws IOException  {
		String[] fields = key.toString().split(Pattern.quote("|"));
		
		try {
			context.write(new Text(value + "\t" + fields[0]), new Text(fields[1]));
		} catch (InterruptedException e) {
			logger.error("TrendingSongsMapper.cleanup() context.write() failed " + fields + e.getMessage());
			e.printStackTrace();
		}
	}

	
}
