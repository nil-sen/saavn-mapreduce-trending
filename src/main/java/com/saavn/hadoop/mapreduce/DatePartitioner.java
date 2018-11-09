package com.saavn.hadoop.mapreduce;

import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

public class DatePartitioner extends Partitioner<Text,Text > implements
    Configurable {

  private Configuration configuration;
  HashMap<String, Integer> dates = new HashMap<String, Integer>();

  /**
   * Set up the dates hash map in the setConf method.
   */
  public void setConf(Configuration configuration) {
    this.configuration = configuration;
    dates.put("2017-12-24", 0);
    dates.put("2017-12-25", 1);
    dates.put("2017-12-26", 2);
    dates.put("2017-12-27", 3);
    dates.put("2017-12-28", 4);
    dates.put("2017-12-29", 5);
    dates.put("2017-12-30", 6);
    
  }

  /**
   * getConf method for the Configurable interface.
   */
  public Configuration getConf() {
    return configuration;
  }

  /**
   * Implement the getPartition method for a partitioner class.
   * This method receives the date as its value. 
   * (It is the output value from the mapper.)
   * It should return an integer representation.
   * Note that trending start date is represented as 0 rather than 1.
   * For this partitioner to work, the job configuration must have been
   * set so that there are exactly equal number of reducers as the number of days.
   */
  public int getPartition(Text key, Text value, int numReduceTasks) {
    return (int) (dates.get(value.toString()));
  }
}
