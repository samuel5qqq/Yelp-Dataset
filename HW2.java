import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class HW2 extends Configured implements Tool{


	private static final String output = "temp";
	
	public static class Map
	extends Mapper<LongWritable, Text, Text, Text>{

		private Text word = new Text(); // type of output key
		private Text friend = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
				String[] mydata = value.toString().split("\t");
				String id = mydata[0];
				
				if(mydata.length == 2){
					
					String[] friends = mydata[1].split(",");
					
					for(String friendindex : friends){
						
						String friendkey ="";
						String temp = friendindex;
						
						if(Integer.parseInt(id) > Integer.parseInt(temp)){
							friendkey = temp+","+id;					
						}
						else
							friendkey = id+","+temp;
						word.set(friendkey);
						friend.set(mydata[1]);
						context.write(word,  friend);
						}
					}
		}
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		
		Text result = new Text();
		private TreeMap<String, Integer> map1 = new TreeMap<String, Integer>();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			String[] data = new String[2];
			String[] friendlist1;
			String[] friendlist2;
			int countint;
			ArrayList<String> temp1 = new ArrayList<String>();
			ArrayList<String> temp2 = new ArrayList<String>();
			ArrayList<String> output = new ArrayList<String>();
			int count = 0;
			StringBuffer temp = new StringBuffer();
			String out;
			for(Text friend : values){
				data[count++] = friend.toString();
			}
			
			
			friendlist1 = data[0].split(",");
			friendlist2 = data[1].split(",");
			
			if(data[0] != null){
				for(String i : friendlist1){
					temp1.add(i);
				}
			}
			
			if(data[1] != null){
				for(String i : friendlist2){
					temp2.add(i);
				}
			}
			
			for (int i = 0; i < temp1.size(); i++){
				for (int j = 0; j < temp2.size(); j++)
					if(temp1.get(i).equals(temp2.get(j))){
						output.add(temp2.get(j));
					}
			}
			
			countint = output.size();
			for (int i = 0; i < output.size(); i++){
				temp.append(output.get(i));
				if(i != output.size()-1){
					temp.append(",");
				}
			}
			if(temp !=null && temp.length() > 0){
				out = temp.toString();
				result.set(out);
				context.write(key,  new Text(result));
			}
			
		}
		
		/*
		class Compare implements Comparator<String>{
			private TreeMap<String, Integer> map;
			
			public Compare(TreeMap<String, Integer> map){
				this.map = map;
			}
			
			public int compare(String a, String b){
				if(map.get(a) <= map.get(b))
					return 1;
				else
					return -1;
			}
		}
		
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException{
			
			int count = 0;
			TreeMap<String, Integer> map2 = new TreeMap<String, Integer>(new Compare(map1));
			map2.putAll(map1);
			
			for(Entry<String, Integer> entry : map2.entrySet()){
				if(count == 10)
					break;
				context.write(new Text(entry.getKey()), new Text(String.valueOf(entry.getValue())));
				count++;
			}
		}
		*/
		
	}
	
	
	
	public static class Filter extends Mapper<LongWritable, Text, LongWritable, Text>{
		
		private final static IntWritable one = new IntWritable(1);
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			int count = 0;
			String[] temp = value.toString().split("\t");
			String[] friend = temp[1].split(",");
			for(String a: friend)
				count++;
			context.write(new LongWritable(count), new Text(temp[0]));
			
		}
	}
	
	public static class Combine extends Reducer<LongWritable, Text, LongWritable, Text>{
		
		int count = 0;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException{
			count = 0;
		}
		
		@Override
		public void reduce(LongWritable key, Iterable<Text> values, Context context){
			
			if(count < 10){
				try{
					for(Text value : values){
						context.write(key, value);
						count++;
					}
				}catch(Exception e){
					
				}
			}
		}
	}
	
	public static class Final extends Reducer<LongWritable, Text, Text, LongWritable>{
		
		int count = 0;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException{
			count = 0;
		}
		
		@Override
		public void reduce(LongWritable key, Iterable<Text> values, Context context){
			
			if(count < 10){
				try{
					for(Text value : values){
						
						if(count == 10)
							break;
						
						context.write(value, key);
						count++;
						
					}
				}catch(Exception e){
					
				}
			}
		}
	}


	
	// Driver program	
	public int run(String[] args) throws Exception{
		
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);

		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		
		//Job1
		Job job1 = Job.getInstance(conf, "MutualFriend");
		job1.setJarByClass(HW2.class);
		
		FileInputFormat.addInputPath(job1, in);
		
		FileOutputFormat.setOutputPath(job1, new Path(output));

		job1.setMapperClass(Map.class);
		job1.setReducerClass(Reduce.class);

		job1.setInputFormatClass(TextInputFormat.class);

		
		if (fs.exists(new Path(output))) {
			fs.delete(new Path(output), true);
		}
		
		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}
		
		job1.setOutputKeyClass(Text.class);
		
		job1.setOutputValueClass(Text.class);
		
		job1.waitForCompletion(true);
		

		
		//Job2
		Job job2 = Job.getInstance(conf, "Filter");
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setJarByClass(HW2.class);
		
		job2.setNumReduceTasks(1);
		
		job2.setMapperClass(Filter.class);
		job2.setReducerClass(Final.class);
		job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
		
		job2.setMapOutputKeyClass(LongWritable.class);
		job2.setMapOutputValueClass(Text.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.addInputPath(job2, new Path(output));

		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		
		
		int res = ToolRunner.run(new Configuration(), new HW2(), args);
		System.exit(res);
		/*
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 2) {
			System.err.println("Usage: WordCount <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "HW2");
		job.setJarByClass(HW2.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);

		job.setOutputValueClass(Text .class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		*/
	}
}

