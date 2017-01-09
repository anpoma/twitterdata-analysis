package com.ann;
 

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.util.regex.Pattern;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*	;
import java.io.IOException;


public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
   
    private Text word = new Text();
    private long numRecords = 0; 
    private String input;
    private boolean caseSensitive = false;
    private Set<String> patternsToSkip = new HashSet<String>();
    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
    private HashMap<String,String> AFINN_map = new HashMap<String,String>();
    

    protected void setup(Mapper.Context context) throws IOException, InterruptedException
    {
    	
    	if(context.getInputSplit() instanceof FileSplit)
    	{
    		this.input = ((FileSplit) context.getInputSplit()).getPath().toString();
    	}
    		else
    		{
    	this.input=	context.getInputSplit().toString();
    			
    	}
    	Configuration config = context.getConfiguration();
    	this.caseSensitive = config.getBoolean("twittersentiment.case.senstive", false);
    	URI[] localPaths = context.getCacheFiles();
    	if(config.getBoolean("twittersentiment.skip.patterns",false))
    	{
            parseSkipFile(localPaths[0]);
          }
    	//create hashmap of the words in afinn to get the score of each word
 try{
    	
    	
    	BufferedReader br = new BufferedReader(new FileReader(new File(localPaths[1].getPath()).getName()));
    	String line="";
    	 
    	while((line = br.readLine())!=null)
    	 
    	{
    	 
    	String splits[] = line.split("\t");
    	 
    	AFINN_map.put(splits[0], splits[1]);
    	 
    	}
    	 
    	br.close();
    
}catch(Exception e){
	 
e.printStackTrace();
	 
	}
        }

        private void parseSkipFile(URI patternsURI) {
          try {
            BufferedReader fis = new BufferedReader(new FileReader(new File(patternsURI.getPath()).getName()));
            String pattern;
            while ((pattern = fis.readLine()) != null) {
              patternsToSkip.add(pattern);
            }
            fis.close();
          } catch (IOException ioe) {
            System.err.println("Caught exception while parsing the cached file '"
                + patternsURI + "' : " + StringUtils.stringifyException(ioe));
          }
        }

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
    	
    	String line = lineText.toString();
    	JSONParser jsonParser = new JSONParser();
    	 
    	try{
    	 
   	JSONObject obj =(JSONObject) jsonParser.parse(line);
    	 
    	String tweet_id = (String) obj.get("id_str");
    	 
    	String tweet_text=(String) obj.get("text");
    	 
    	String[] splits = tweet_text.toString().split(" ");
    	 
    	int sentiment_sum=0;
    	 
   
    	 for (String word : WORD_BOUNDARY.split(tweet_text)) {
    	        if (word.isEmpty() || patternsToSkip.contains(word) && (AFINN_map.containsKey(word))) {
    	          
    	    	Integer x=new Integer(AFINN_map.get(word));
    	    	sentiment_sum+=x;
    	    	 
    	    	}
    		 
    	        }
    	 
    	context.write(new Text("result") ,new IntWritable(sentiment_sum));
    
    	}catch(Exception e){
    	 
    	e.printStackTrace();
    	 
    	}
    	 
    	}
    
    	 
  }
