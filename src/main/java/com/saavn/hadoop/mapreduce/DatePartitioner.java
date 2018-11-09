package com.saavn.hadoop.mapreduce;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.saavn.hadoop.util.DateUtil;

public class DatePartitioner extends Partitioner<Text, Text> implements Configurable {

	private static final Logger logger = LoggerFactory.getLogger(DatePartitioner.class);

	private Configuration configuration;
	HashMap<String, Integer> dates = new HashMap<String, Integer>();

	private static final String TRENDING_START_DATE = "trending.start.date";
	private static final String TRENDING_END_DATE = "trending.end.date";
	private static final String DATE_FORMAT = "yyyy-mm-dd";

	private Date trendingStartDate = null;
	private Date trendingEndDate = null;
	private DateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);

	/**
	 * Set up the dates hash map in the setConf method.
	 */
	public void setConf(Configuration configuration) {
		this.configuration = configuration;

		try {
			trendingStartDate = dateFormat.parse(this.configuration.get(TRENDING_START_DATE));
			trendingStartDate = DateUtil.addDays(trendingStartDate, -1);
			trendingEndDate = dateFormat.parse(this.configuration.get(TRENDING_END_DATE));
		} catch (ParseException e) {
			logger.error("DatePartitioner.setConf() dateFormat.parse() failed " + e.getMessage());
			e.printStackTrace();
		}

		String partitionDate = null;
		int partitionNumber = 0;

		while (trendingStartDate.before(trendingEndDate)) {
			partitionDate = dateFormat.format(trendingStartDate);
			dates.put(partitionDate, partitionNumber);
			partitionNumber = partitionNumber + 1;
			trendingStartDate = DateUtil.addDays(trendingStartDate,1);
		}

	}

	/**
	 * getConf method for the Configurable interface.
	 */
	public Configuration getConf() {
		return configuration;
	}

	/**
	 * Implement the getPartition method for a partitioner class. This method
	 * receives the date as its value. (It is the output value from the mapper.) It
	 * should return an integer representation. Note that trending start date is
	 * represented as 0 rather than 1. For this partitioner to work, the job
	 * configuration must have been set so that there are exactly equal number of
	 * reducers as the number of days.
	 */
	public int getPartition(Text key, Text value, int numReduceTasks) {
		return (int) (dates.get(value.toString()));
	}
}
