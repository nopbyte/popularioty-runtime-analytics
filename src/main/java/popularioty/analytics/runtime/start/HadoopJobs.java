package popularioty.analytics.runtime.start;


import java.io.File;
import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import popularioty.analytics.runtime.mappers.GenericEntityMapper;
import popularioty.analytics.runtime.mappers.GenericEntityReducer;
import popularioty.analytics.runtime.writable.RuntimeEdge;
import popularioty.analytics.runtime.writable.RuntimeKey;
import popularioty.analytics.runtime.writable.RuntimeVote;

public class HadoopJobs {
	
	public static long DEFAULT_SINCE = 0;// System.currentTimeMillis()-(3600*24*1000);
	
	public static long DEFAULT_UNTIL= System.currentTimeMillis();
	
	public static void main(String[] args) throws Exception {
		
		long since = DEFAULT_SINCE;
		long until = DEFAULT_UNTIL;
		String s = readFile("/opt/scripts/last_runtime.txt");
		if(s!=null && !s.equals("")){
			since = Long.parseLong(s.trim());
		}
		
		if(args.length>3)
		{
			since = Long.parseLong(args[2]);
			until = Long.parseLong(args[3]);
		}
		
		Configuration conf = new Configuration();
		//since one day ago...
		
		conf.setLong("documents-since", since);
		conf.setLong("documents-until", until);
		
		Job job = Job.getInstance(conf, "runtime");
		// This decrease the number of times the ES and CB clients have to join the ES and CB clusters respectively
		//job.setNumReduceTasks(-1);
		job.setJarByClass(HadoopJobs.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(RuntimeKey.class);
		job.setMapOutputValueClass(RuntimeVote.class);
		
		
		job.setOutputKeyClass(RuntimeKey.class);
		job.setOutputValueClass(RuntimeEdge.class);

		job.setMapperClass(GenericEntityMapper.class);
		job.setReducerClass(GenericEntityReducer.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		
		job.waitForCompletion(true);
	}
	
	public static String readFile( String file ) {
		try{
			String content = new Scanner(new File(file)).useDelimiter("\\Z").next();
			return content;
		}catch(IOException x){
			System.out.println("****************************no previous timestamp******************************************");
			System.out.println("exception while reading last timestamp file "+x.getMessage());
		}
		return null;
		
	}
	
	
	

}