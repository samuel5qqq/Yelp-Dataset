import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class MutualFriend {


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
					/*
					for (int i = 0; i < friends.length; i++) {
						
						String friendkey;
						String temp = friends[i];
						
						if(Integer.parseInt(id) > Integer.parseInt(temp)){
							friendkey = temp+""+id;					
						}
						else
							friendkey = id+""+temp;
						
						word.set(friendkey); // set word as each input keyword
						friend.set(mydata[1]);
						context.write(word, friend);
					}
					*/
					
				}
			}
			
		}
	
	public static class Reduce 
	extends Reducer<Text, Text, Text, Text> {

		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			String[] data = new String[2];
			String[] friendlist1;
			String[] friendlist2;
			
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
			
			for (int i = 0; i < output.size(); i++){
				temp.append(output.get(i));
				if(i != output.size()-1){
					temp.append(",");
				}
			}
			if(temp !=null && temp.length() > 0){
				out = temp.toString();
				result.set(out);
				context.write(key, result);
			}
			
		}
	}


	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 2) {
			System.err.println("Usage: WordCount <in> <out>");
			System.exit(2);
		}


		Job job = new Job(conf, "mutualfriend");
		FileSystem fs = FileSystem.get(conf);
		job.setJarByClass(MutualFriend.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);


		if (fs.exists(new Path(otherArgs[1]))) {
			fs.delete(new Path(otherArgs[1]), true);
		}

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
