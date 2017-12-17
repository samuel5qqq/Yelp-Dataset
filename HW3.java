import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
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


public class HW3 extends Configured implements Tool {
	
	private static final String output = "temp";

	public static class BusinessMap extends Mapper<LongWritable, Text, Text, Text>{

		private final static IntWritable one = new IntWritable(1);

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			
			String[] data = value.toString().split("::");
			String id;
			String address;
			String categories;
			if(data.length == 3){
				id = data[0];
				address = data[1];
				categories = data[2];
				String temp = address+","+categories;
				context.write(new Text(id), new Text(temp));
			}

		}
	}
	
	public static class ReviewMap extends Mapper<LongWritable, Text, Text, Text>{

		private final static IntWritable one = new IntWritable(1);

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			
			String[] data = value.toString().split("::");
			String review_id;
			String user_id;
			String business_id;
			String rating;
			if(data.length == 4){
				review_id = data[0];
				user_id = data[1];
				business_id = data[2];
				rating = data[3];
				context.write(new Text(business_id), new Text(rating));
			}

		}
	}
	
	public static class TempMap extends Mapper<LongWritable, Text, Text, Text>{


		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] data = value.toString().split("\t");
			if(data.length == 2){
				String out = "number:"+data[1];
				context.write(new Text(data[0]), new Text(out));
			}
		}
	}

	public static class Reduce extends Reducer<Text,Text,Text,Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			
			float count = 0;
			float sum = 0;
			float average = 0;
			
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
	
	public static class Reduce1 extends Reducer<Text,Text,Text,Text> {
		
		private TreeMap<String, Float> map1 = new TreeMap<String, Float>();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			
			
			String out="";
			String finalout = key.toString();
			String rating="";
			int count1 = 0, count2 = 0;
			for(Text temp1 : values){
				String tempstring1 = temp1.toString();
				for(Text temp2 : values){
					String tempstring2 = temp2.toString();
					if(count1 < 1 && !(tempstring1.equals(tempstring2))){
						if(tempstring1.contains("number")){
							String[] number = tempstring1.split(":");
							rating = number[1];
							out = tempstring2;
							finalout = finalout + out;
							map1.put(finalout, Float.parseFloat(rating));
							count1++;
						}
						if(tempstring2.contains("number")){
							String[] number = tempstring2.split(":");
							rating = number[1];
							out = tempstring1;
							finalout = finalout + out;
							map1.put(finalout, Float.parseFloat(rating));
							count1++;
						}
					}
				}
			}
			

			
		}
		
		class Compare implements Comparator<String>{
			private Map<String, Float> map;
			
			public Compare(Map<String, Float> map){
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
			TreeMap<String, Float> map2 = new TreeMap<String, Float>(new Compare(map1));
			map2.putAll(map1);
			
			for(Entry<String, Float> entry : map2.entrySet()){
				if(count == 10)
					break;
				context.write(new Text(entry.getKey()), new Text(String.valueOf(entry.getValue())));
				count++;
			}
		}
		
		
		
	}
	
	public int run(String[] args) throws Exception{
		
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);

		Path path1 = new Path(args[0]);
		Path path2 = new Path(args[1]);
		Path out = new Path(args[2]);
		
		Job job1 = new Job(conf, "HW3");
		job1.setJarByClass(HW3.class);
		job1.setMapperClass(ReviewMap.class);
		job1.setReducerClass(Reduce.class);

		job1.setOutputKeyClass(Text.class);
		// set output value type
		job1.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job1, path1);
		FileOutputFormat.setOutputPath(job1, new Path(output));
		if (fs.exists(new Path(output))) {
			fs.delete(new Path(output), true);
		}
		
		if (fs.exists(new Path(args[2]))) {
			fs.delete(new Path(args[2]), true);
		}

		job1.waitForCompletion(true);
		
		
		Job job2 = Job.getInstance(conf, "Filter");
		job2.setJarByClass(HW3.class);
		
		job2.setMapperClass(BusinessMap.class);
		job2.setMapperClass(TempMap.class);
		
		MultipleInputs.addInputPath(job2, path2, TextInputFormat.class, BusinessMap.class);
		MultipleInputs.addInputPath(job2, new Path(output), TextInputFormat.class, TempMap.class);
		
		job2.setReducerClass(Reduce1.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job2, out);
		
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
		
		return 0;
	}

	// Driver program
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new HW3(), args);
		System.exit(res);
	}
}
