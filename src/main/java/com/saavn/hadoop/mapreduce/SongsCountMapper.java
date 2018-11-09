package com.saavn.hadoop.mapreduce;


import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.saavn.hadoop.util.TrendingSongsUtil;


/**
 * Created by nsen
 */
public class SongsCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private static final Logger logger = LoggerFactory.getLogger(SongsCountMapper.class);

	private static final String TRENDING_DATE = "trending.date";
	
	private Date trendingDate = null; 
	private Date trendingEndDate = null; 
    private DateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd");
    
	@Override
    public void setup(Context context) {		
        try {
        	trendingDate = dateFormat.parse(context.getConfiguration().get(TRENDING_DATE));
        	trendingDate = TrendingSongsUtil.addDays(trendingDate,-1);	
        	trendingEndDate = TrendingSongsUtil.addDays(trendingDate,7);
    	} catch (ParseException e) {
    		e.printStackTrace();
    	}
    }
	
	
    
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException {

        // Parse line and get songID, userID, etc
        String[] fields = value.toString().split(",");
        if (fields.length >= 5) {
        	String songID = fields[0];
            //String userID = fields[1];
            //String timestamp = fields[2];
            String hour = fields[3];
            String date = fields[4];
            
            if (songID != null) {
            	if(!("(null)".equals(songID) || "null".equals(songID) || "".equals(songID.trim()))) {
            		Date recordDate = null;
                                       
                    try {
                    	recordDate = dateFormat.parse(date);
	               	} catch (ParseException e) {
	               		logger.error("SongsCountMapper.map() dateFormat.parse() failed " + date + e.getMessage());
	               		e.printStackTrace();
	               	}
                   
                    // consider entire 24 hours of day for trending
                    if((recordDate != null && 
                    		(recordDate.compareTo(trendingDate) == 0 
                    		|| (recordDate.after(trendingDate) && recordDate.before(trendingEndDate)))) 
                    		&& (hour != null && Integer.parseInt(hour) >= 0 && Integer.parseInt(hour) <= 23)) { 
	                   	try {
	                   		 // Output SongID + count = 1
	                   		context.write(new Text(songID.trim() + "|" + date), new IntWritable(1));
	                   	} catch (InterruptedException e) {
	                   		logger.error("SongsCountMapper.map() context.write() failed " + songID + "|" + date);
	                   		e.printStackTrace();
	                   	}	                   
	               	}
	           	}
            }
        }   
    }

}

