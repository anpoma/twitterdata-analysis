package com.ann;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text; 

public  class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    
	public void reduce(Text word, IntWritable count, Context context)
			throws IOException, InterruptedException
	{
		context.write(word, count);
	}
	
	
	/*
	public void reduce(Text word, Iterable<IntWritable> instances, Context context)
			throws IOException, InterruptedException
	{
		int sum = 0;

		// Sum up the instances of the current word.
		for (IntWritable instance : instances) {
			sum += instance.get();
		}
		
		// Write the word and count to output.
		context.write(word, new IntWritable(sum));
	}
	*/

    }
  

