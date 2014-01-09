import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

public class MultiMain 
{

	
	private static ArrayList<String> frequents = new ArrayList<String>();
	
	private static HashMap<String, Integer> fbuckets = new HashMap<String, Integer>();
	private static HashMap<String, Integer> sbuckets = new HashMap<String, Integer>();
	
	private static final int threshold = 3000;
	
	public static class MapperItemSets extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		@Override
			    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException, NullPointerException
			    {	        
			        String items []  = value.toString().split(" ");
			        
			        for(int r=0; r<items.length; r++)
					{
						for(int q = r+1 ; q<items.length ; q++)
						{
							int a,b;
							a=Integer.parseInt(items[r]);
							b=Integer.parseInt(items[q]);
							
							String fhash = new String(String.valueOf( ((a*b)%30011) ) );
							String shash = new String(String.valueOf( ((a*1009 + b)%30011) ) );
							
							if(fbuckets.containsKey(fhash))
								fbuckets.put(fhash, fbuckets.get(fhash)+1);
							else
								fbuckets.put(fhash, 1);
							
							if(sbuckets.containsKey(shash))
								sbuckets.put(shash, sbuckets.get(shash)+1);
							else
								sbuckets.put(shash, 1);
								
						}
						context.write(new Text(items[r]), new IntWritable(1));
					}
			        
			    }
			    
			}
	
	public static class ReducerItemSets extends Reducer<Text, IntWritable, Text, IntWritable> {
	    
		
		@Override
			    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
			    {
			    	int count = 0;
			    	
			    	for(IntWritable c : values)
			    	{
			    		count+=c.get();
			    	}
			    	
			    	if(count>=threshold)
			    	{
			    		frequents.add(key.toString());
			    		context.write(key, new IntWritable(count));
			    
			    	}
			    	
			    }
			    	  
			}
	
public static class MapperPairs extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		@Override
			    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException, NullPointerException
			    {	
			String items []  = value.toString().split(" ");
	        
	        for(int r=0; r<items.length; r++)
			{
				for(int q = r+1 ; q<items.length ; q++)
				{
										
						int a,b;
						a=Integer.parseInt(items[r]);
						b=Integer.parseInt(items[q]);
						
						String fhash = new String(String.valueOf( ((a*b)%30011) ) );
						String shash = new String(String.valueOf( ((a*1009 + b)%30011) ) );
						
						if(frequents.contains(items[r])&&frequents.contains(items[q])&&fbuckets.get(fhash)>=threshold&&sbuckets.get(shash)>=threshold)
							{ 
										context.write(new Text(items[r]+","+items[q]), new IntWritable(1));
							}
				     	}
				     }
			      }
			   }
	
	public static class ReducerPairs extends Reducer<Text, IntWritable, Text, IntWritable> {
	    
		@Override
			    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
			    {
			int count = 0;
	    	
	    	for(IntWritable c : values)
	    	{
	    		count+=c.get();
	    	}
	    	
	    	if(count>=threshold)
	    	
	    		context.write(key, new IntWritable(count));
			
			    	  
			}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
Configuration conf = new Configuration();
		
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	
		Job job1 = new Job(conf, "MultiHash Sets");
		 
        job1.setJarByClass(MultiMain.class);
        
        //job.setNumReduceTasks(1);
        
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        job1.setMapperClass(MapperItemSets.class);
       
        job1.setReducerClass(ReducerItemSets.class);
        
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));

        if(!job1.waitForCompletion(true))
        	System.exit(0);
              
        Job job2 = new Job(conf, "MultiHash Pairs");
		
        job2.setJarByClass(MultiMain.class);
        
        //job.setNumReduceTasks(1);
        
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        job2.setMapperClass(MapperPairs.class);
       
        job2.setReducerClass(ReducerPairs.class);
        
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job2, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]+"/pairs"));

        if(!job2.waitForCompletion(true))
        	System.exit(0);
        
	}

}
