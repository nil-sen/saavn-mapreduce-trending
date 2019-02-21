# saavn-mapreduce-trending
An Overview of the Problem and the Data sets

Problem statement
Write and execute a MapReduce program to figure out the top 100 trending songs from Saavn’s stream data, on a daily basis, for the week December 25-31. Although this is a real-time streaming problem, you may use all the data till the (n−1)th day to calculate your output for the n th day, i.e. you may consider all the stream data till 24 December (included) in your program to find the trending songs for 25 December and so on.

Definition of trending
The term ‘trending songs’ may be defined loosely as those songs that have gathered relatively high numbers of streams within small time windows (e.g. the last four hours) and have also shown positive increases in their stream growth rates.

How is a stream defined at Saavn?
A stream is a record of a user playing a song. Each stream is represented as a tuple with the following attributes: (song ID, user ID, timestamp, hour, date)

Each tuple consists of the song ID of the streamed song, the user ID of the user who streamed the song, the timestamp (Unix) of the stream, the hour of streaming, and the date of streaming.

Data
- The file https://s3.amazonaws.com/mapreduce-project-bde/part-00000 contains one month(December) of stream records. Please note that this file is huge (~44GB) and will consume a lot of your internet bandwidth if you choose to download it onto your local machine. In the next session, you will learn how to run a MapReduce job that takes input directly from S3. 

- The file https://s3.amazonaws.com/mapreduce-project-bde/trending_data_daily.csv contains the trending songs for each day of December, as calculated by Saavn. You may compare your output with these and improve your algorithm to obtain a better match.

- The file https://s3.amazonaws.com/mapreduce-project-bde/saavn_sample_data.txt contains a sample of 10 million stream records from the original dataset. You may use this to run simple jobs and get an idea of the data.

# saavn-mapreduce-trending
Algorithm used to arrive at top 100 trending songs for each day 25-31 Dec 2017 :

1. To get the trending songs for a particular day (say n) all songs streamed on previous day (n-1) th day throughout entire period of 24 hours is taken into account. So for getting trending songs for 25th Dec, all songs played on 24th Dec (0-23 hours) is taken into account.
2. Two mapreduce jobs used using chaining of jobs. The output of 1st job is provided as the input of 2nd job.
3. Number of reduce tasks calculated based on the start trending date and end trending date
4. First job consists of mapper (SongsCountMapper.java) and reducer (SongsCountReducer.java) as well as combiner (SongsCountReducer.java) and partitioner (CountPartitioner.java).
5. Mapper is used to output each songId concatenated with date as key and count 1 as value
6. Custom partitioner created based on hash value of key
7. Reducer is used to sum the count of each key (songId plus date)
8. Second job accepts output from 1st job as input
9. Second job consists of mapper (TrendingSongsMapper.java) and reducer (TrendingSongsReducer.java) as well as partitioner (DatePartitioner.java)
10. Mapper outputs count plus songid as key and date as value
11. Custom partitioner used based on date. So there will be 7 partitions in this case
12. Java Treemap is used to sort all the songs based on count in reducer. Treemap sorts the elements based on key
13. Count of song is put as key and songId as value in Treemap
14. Whenever treemap size is greater than top.n count say 100 the first element is removed. This will provide us a treemap with only top 100 records sorted in ascending order
15. In Reducer cleanup we are writing treemap in descending order of values with output Nullwritable as key and Text as value (which is songId)

Steps involved to arrive at the output files :

1. Run hadoop jar command to run map reduce jobs
hadoop jar mapreduce-1.0.0.jar com.saavn.hadoop.TrendingSongsDriver -Dtop.n=100 -Dtrending.start.date=2017-12-25 -Dtrending.end.date=2017-12-31 <INPUT_FILE> <OUTPUT_DIRECTORY>
where
top.n is the number of trending song count e.g 100 top trending songs
trending.start.date is the date starting from trending needs to be calculated e.g 2017-12-25
trending.end.date is the date till which trending songs needs to be calculated e.g 2017-12-31
There will be 2 sub directories created under output directory as
below
          1) <OUTPUT_DIRECTORY>/count - this will have the output from 1st mapreduce job which will be the input for 2nd mapreduce job
          2) <OUTPUT_DIRECTORY>/topN - this will have the final output (aka 2nd mapreduce job). This will have separate files generated           for each day having sorted list of top 100 song Ids separated by newline

2. Get all the files from topN sub directory using below command hadoop fs -get <OUTPUT_DIRECTORY>/topN/*

3. Download the above output files in Windows laptop using FileZilla and rename as below
1) part-r-00000 --> 25.txt
2) part-r-00001 --> 26.txt
3) part-r-00002 --> 27.txt
4) part-r-00003 --> 28.txt
5) part-r-00004 --> 29.txt
6) part-r-00005 --> 30.txt
7) part-r-00006 --> 31.txt

References :
1) https://hadoop.apache.org/docs/stable/hadoop-mapreduceclient/hadoop-mapreduce-client-core/MapReduceTutorial.html
2) https://blogs.msdn.microsoft.com/avkashchauhan/2012/03/29/how-tochain-multiple-mapreduce-jobs-in-hadoop/
3) https://content.pivotal.io/blog/hadoop-mapreduce-can-transformhow-you-build-top-ten-lists
