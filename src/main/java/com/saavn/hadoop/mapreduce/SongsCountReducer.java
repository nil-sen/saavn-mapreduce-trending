package com.saavn.hadoop.mapreduce;



import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by nsen
 */
public class SongsCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private static final Logger logger = LoggerFactory.getLogger(SongsCountReducer.class);
	private static final IntWritable SUM = new IntWritable();
		
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException {

        int count = 0;
        for (IntWritable value : values) {
            count = count + value.get();
        }

        try {
        	SUM.set(count);
			context.write(key, SUM);
		} catch (InterruptedException e) {
			logger.error("SongsCountReducer.reduce() context.write() failed : " + e.getMessage());
			e.printStackTrace();
		}
    }
}

