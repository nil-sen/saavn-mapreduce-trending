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

import com.saavn.hadoop.util.DateUtil;


/**
 * Created by nsen
 */
public class SongsCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private static final Logger logger = LoggerFactory.getLogger(SongsCountMapper.class);
	private static final IntWritable ONE = new IntWritable(1);
	private static final Text KEY = new Text();
    
	
	private static final String TRENDING_START_DATE = "trending.start.date";
	private static final String TRENDING_END_DATE = "trending.end.date";
	private static final String PIPE = "|";
	private static final String DATE_FORMAT = "yyyy-mm-dd";
	
	private Date trendingStartDate = null; 
	private Date trendingEndDate = null; 
    private DateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
    
	@Override
    public void setup(Context context) {		
        try {
        	trendingStartDate = dateFormat.parse(context.getConfiguration().get(TRENDING_START_DATE));
        	trendingStartDate = DateUtil.addDays(trendingStartDate,-1);	
        	trendingEndDate = dateFormat.parse(context.getConfiguration().get(TRENDING_END_DATE));        		
    	} catch (ParseException e) {
    		e.printStackTrace();
    	}
    }
	
	
    
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException {

        // Parse line and get songID, hour, date, etc
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
                    		(recordDate.compareTo(trendingStartDate) == 0 
                    		|| (recordDate.after(trendingStartDate) && recordDate.before(trendingEndDate)))) 
                    		&& (hour != null && Integer.parseInt(hour) >= 0 && Integer.parseInt(hour) <= 23)) { 
	                   	try {
	                   		 // Output key => SongID + pipe + date , value => count = 1
	                   		KEY.set(songID.trim() + PIPE + date);
	                   		context.write(KEY, ONE);
	                   	} catch (InterruptedException e) {
	                   		logger.error("SongsCountMapper.map() context.write() failed " + songID + PIPE + date);
	                   		e.printStackTrace();
	                   	}	                   
	               	}
	           	}
            }
        }   
    }

}

