package com.saavn.hadoop.mapreduce;

import java.io.IOException;

import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrendingSongsMapper extends Mapper<Text, Text, Text, Text> {

	private static final Logger logger = LoggerFactory.getLogger(TrendingSongsMapper.class);
	
	private static final String TAB = "\t";
	private static final String PIPE = "|";
	
	private static final Text KEY = new Text();
	private static final Text VAL = new Text();

	@Override
	public void map(Text key, Text value, Context context) throws IOException  {
		String[] fields = key.toString().split(Pattern.quote(PIPE));
		
		try {
			KEY.set(value + TAB + fields[0]);
			VAL.set(fields[1]);
			context.write(KEY, VAL);
		} catch (InterruptedException e) {
			logger.error("TrendingSongsMapper.map() context.write() failed " + fields + e.getMessage());
			e.printStackTrace();
		}
	}

	
}
