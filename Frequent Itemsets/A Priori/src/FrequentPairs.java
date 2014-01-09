import java.io.IOException;
import java.util.ArrayList;

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

public class FrequentPairs {

	private static ArrayList<String> frequents = new ArrayList<String>();
	private static final int threshold = 3000;
	private static int pcount =0;
	
	public static class MapperItemSets extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		@Override
			    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException, NullPointerException
			    {	        
			        String items []  = value.toString().split(" ");
			        
			        for (String j : items)
			        context.write(new Text(j), new IntWritable(1));
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
				String [] input = value.toString().split(" ");
				
				for(int r=0; r<input.length-1; r++)
				{
					for(int q = r+1 ; q<input.length ; q++)
					{
						if(frequents.contains(input[r])&&frequents.contains(input[q]))
							{
							pcount++;
							//System.out.println(input[r]+","+input[q]);
							context.write(new Text(input[r]+","+input[q]), new IntWritable(1));
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
	    	{
	    		frequents.add(key.toString());
	    		context.write(key, new IntWritable(count));
			    }
			    	  
			}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
		
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	
		Job job1 = new Job(conf, "Frequent Item Sets");
		 
        job1.setJarByClass(FrequentPairs.class);
        
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
              
        Job job2 = new Job(conf, "Frequent Pairs");
		
        job2.setJarByClass(FrequentPairs.class);
        
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
        
        System.out.println(pcount);

	}

}
