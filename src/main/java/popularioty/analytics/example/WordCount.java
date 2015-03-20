package popularioty.analytics.example;


import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import popularioty.commons.exception.PopulariotyException;
import popularioty.commons.services.search.FeedbackReputationSearch;

public class WordCount {
	
	
	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		
		
		private Text word = new Text();

		public static java.util.Map<String,Object>  getSettings()
		{
			java.util.Map<String,Object>  settings = new HashMap<String,Object>();
			settings.put("storage.engine","couch_base");
			settings.put("couchbase.host","[\"192.168.56.105\"]");
			settings.put("couchbase.timeout.value","2");
			settings.put("couchbase.timeout.timeunit","2");		
			//settings.put("couchbase.port","8092");
			settings.put("feedback.bucket","feedback");
			settings.put("search.engine","elastic_search");
			settings.put("client.transport.host","192.168.56.105");
			settings.put("client.transport.port","9300");
			settings.put("index.aggregated","reputation_aggregations");
			settings.put("index.feedback","feedback");
			settings.put("index.metafeedback","meta_feedback");
			settings.put("index.subreputation","subreputation");
			
			return settings;
		}
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			
			word.set("hello");
			context.write(word, one);
			/*String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(word, one);
			}*/
			
			
		}

	}

	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			/*int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
			*/
			FeedbackReputationSearch fedback = ClassicSingleton.getInstance().fedback;
			
			try {
				List<String> r = fedback.getFeedbackByEntity("test_9580fdd0-856e-444b-af5d-be89a022b2db",  "test_type", null, 0, 1);
				key.set(r.get(0));
				context.write(key, new IntWritable(1));
				
				
			} catch (PopulariotyException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		
		/*SearchProvider prov = SearchEngineFactory.getSearchProvider("elastic_search");
		if(prov==null)
			System.err.println("provider is null!");
		else{
		prov.toString();
		FeedbackReputationSearch fedback = new FeedbackReputationSearch(Map.getSettings(), prov);
		prov.init(Map.getSettings());
		try {
			List<String> r = fedback.getFeedbackByEntity("test_9580fdd0-856e-444b-af5d-be89a022b2db",  "test_type", null, 0, 1);
			System.out.println(r.get(0));
			
		} catch (PopulariotyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		}*/
		
		Configuration conf = new Configuration();

		Job job = new Job(conf, "wordcount");
		// This decrease the number of times the ES and CB clients have to join the ES and CB clusters respectively
		//job.setNumReduceTasks(-1);
		job.setJarByClass(WordCount.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}