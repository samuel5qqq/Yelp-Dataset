import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.*;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HW4 extends Configured implements Tool{
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		HashSet<String> stopWordsSet = new HashSet<String>();

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text(); // type of output key
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			Path part = new Path(conf.get("ReadFile"));
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] list = fs.listStatus(part);
			for (FileStatus status : list) {
				
				Path pt = status.getPath();
				
				readFile(pt, fs);

			}
		}
		
		private void readFile(Path filepath, FileSystem fs) {
			try {
				
				BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(filepath)));

				String stopwords = null;
				while ((stopwords = bufferedReader.readLine()) != null) {
					String[] line = stopwords.split("::");
					if(line.length == 3){
						if(line[1].toLowerCase().contains("palo alto")){
							stopWordsSet.add(line[0]);
						}
					}
				}

			} catch (Exception e) {
				System.err.println("Exception while reading stop words file: "
						+ e.getMessage());
			}

		}
		

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			
			String[] line = value.toString().split("::");
			
			if(line.length == 4){
				if(stopWordsSet.contains(line[2])){
					context.write(new Text(line[1]), new Text(line[3]));
				}
			}

		}
	}
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			float count = 0;
			float sum = 0;
			float average = 0;
			String a="";
			for(Text temp : values){
				sum += Float.parseFloat(temp.toString());
				count++;
			}
			
			if(count !=0 ){
				average = sum/count;
				context.write(key, new Text(String.valueOf(average))); // create a pair <keyword, number of occurences>
			}
		}
}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); // get all args
		if (args.length != 3) {
			System.err.printf("Usage: %s needs two arguments <input> <output> <stopwordsfile> files\n");
			System.exit(2);
		}
		
		conf.set("ReadFile", otherArgs[1]);
		FileSystem fs = FileSystem.get(conf);
		
		Job job = new Job(conf, "HW4");
		job.setJarByClass(HW4.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
 
		if (fs.exists(new Path(otherArgs[2]))) {
			fs.delete(new Path(otherArgs[2]), true);
		}

		int returnValue = job.waitForCompletion(true) ? 0 : 1;

		if (job.isSuccessful()) {
			System.out.println("Job was successful");
		} else if (!job.isSuccessful()) {
			System.out.println("Job was not successful");
		}

		return returnValue;
	}

	public static void main(String[] args) throws Exception {
		
		int exitCode = ToolRunner.run(new HW4(), args);
		System.exit(exitCode);

	}

}