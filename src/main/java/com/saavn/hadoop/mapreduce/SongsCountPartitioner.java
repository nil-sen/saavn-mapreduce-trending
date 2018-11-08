package com.saavn.hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class SongsCountPartitioner extends Partitioner<Text,IntWritable >  {

	@Override
    public int getPartition(Text key, IntWritable value, int numReduceTasks) {
        return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }

	
}
