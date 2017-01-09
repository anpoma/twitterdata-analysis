package com.ann;


import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
public class Drive extends Configured implements Tool {

 private static final Logger LOG = Logger.getLogger(Drive.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Drive(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    
    
    
	Job job = new Job(getConf(),"wordcount" );
	
	 for (int i = 0; i < args.length; i += 1) {
		 //add file containing stop words to distributed cache
	      if ("-skip".equals(args[i])) {
	        job.getConfiguration().setBoolean("twittersentiment.skip.patterns", true);
	        i += 1;
	        job.addCacheFile(new Path(args[i]).toUri());
	        // this demonstrates logging
	        LOG.info("Added file to the distributed cache: " + args[i]);
	      }
	  	if ("-no_case".equals(args[i])) {
			job.getConfiguration().setBoolean("twittersentiment.case.sensitive", true);
		}

	      if ("-afinn".equals(args[i])) {
		        job.getConfiguration().setBoolean("twittersentiment.afinn.patterns", true);
		        i += 1;
		        job.addCacheFile(new Path(args[i]).toUri());
		        // this demonstrates logging
		        LOG.info("Added file to the distributed cache: " + args[i]);
		      }
	    }
	 
	job.setJarByClass(this.getClass());
	job.setJobName(this.getClass().getName());
    
    // MultiLineJsonInputFormat for getting JSON object in string form as map input
	job.setInputFormatClass(MultiLineJsonInputFormat.class);
	MultiLineJsonInputFormat.setInputJsonMember(job, "id");
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(com.ann.Map.class);
    job.setReducerClass(com.ann.Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    return job.waitForCompletion(true) ? 0 : 1;
  }
}
