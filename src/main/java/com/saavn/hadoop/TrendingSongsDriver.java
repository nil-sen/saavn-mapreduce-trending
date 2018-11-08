package com.saavn.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.saavn.hadoop.mapreduce.SongsCountMapper;
import com.saavn.hadoop.mapreduce.SongsCountPartitioner;
import com.saavn.hadoop.mapreduce.SongsCountReducer;
import com.saavn.hadoop.mapreduce.TrendingSongsMapper;
import com.saavn.hadoop.mapreduce.TrendingSongsReducer;


/**
 * Trending Songs of nth day is defined by songs streamed for 24 hours on
 * (n-1)th day Input parameters trending.date (nth day) and top.n (count 100) We
 * need two jobs here: - one to count the songs streamed on (n-1)th day based on
 * songID -- used map and reduce job alongwith combiner and partitioner similar
 * to word count job - one to sort above songs data (top.n) -- used map and
 * reduce job with treemap 
 */
public class TrendingSongsDriver extends Configured implements Tool {

	public static String usage() {
		return "usage : <dataDir> <outputDir> -D top.n=<topNCount>  -D trending.date=<TRENDING_DATE> ";
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new TrendingSongsDriver(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		if (args == null || args.length < 2) {
			System.out.println(usage());
			System.exit(1);
		}

		String jobName = "saavn.songs";
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		Path songCountPath = new Path(outputPath, "count");
		Path topSongPath = new Path(outputPath, "topN");

		Configuration conf1 = this.getConf();

		Job job1 = Job.getInstance(conf1, jobName + "_1");
		job1.setJarByClass(TrendingSongsDriver.class);

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job1, inputPath);
		FileOutputFormat.setOutputPath(job1, songCountPath);

		job1.setMapperClass(SongsCountMapper.class);
		job1.setCombinerClass(SongsCountReducer.class);
		job1.setReducerClass(SongsCountReducer.class);

		// No need only 1 reducer since we supply partitioner

		job1.setPartitionerClass(SongsCountPartitioner.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		Configuration conf2 = getConf();

		Job job2 = Job.getInstance(conf2, jobName + "_2");
		job2.setJarByClass(TrendingSongsDriver.class);

		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job2, songCountPath);
		FileOutputFormat.setOutputPath(job2, topSongPath);

		job2.setNumReduceTasks(1);

		////////////////////////////////
		job2.setMapperClass(TrendingSongsMapper.class);
		job2.setReducerClass(TrendingSongsReducer.class);

		job2.setMapOutputKeyClass(NullWritable.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(NullWritable.class);
		job2.setOutputValueClass(Text.class);
				
		ControlledJob controlledJob1 = new ControlledJob(conf1);
		controlledJob1.setJob(job1);

		ControlledJob controlledJob2 = new ControlledJob(conf2);
		controlledJob2.setJob(job2);
		// make job2 dependent on job1
		controlledJob2.addDependingJob(controlledJob1);

		// add the job to the job control
		JobControl jobControl = new JobControl("jobChain");
		jobControl.addJob(controlledJob1);
		jobControl.addJob(controlledJob2);
		Thread jobControlThread = new Thread(jobControl);
		jobControlThread.start();

		while (!jobControl.allFinished()) {
			System.out.println("Jobs in waiting state: " + jobControl.getWaitingJobList().size());
			System.out.println("Jobs in ready state: " + jobControl.getReadyJobsList().size());
			System.out.println("Jobs in running state: " + jobControl.getRunningJobList().size());
			System.out.println("Jobs in success state: " + jobControl.getSuccessfulJobList().size());
			System.out.println("Jobs in failed state: " + jobControl.getFailedJobList().size());
			try {
				Thread.sleep(30000);
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		System.out.println("done");
		jobControl.stop();

		System.exit(0);
		return (job1.waitForCompletion(true) ? 0 : 1);

	}
}
