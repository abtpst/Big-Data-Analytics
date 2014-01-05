import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class AbtPager {

	public static class AbtPagerMapperOne extends Mapper<Text, Text, Text, Text> {
		
		@Override
			    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException, NullPointerException
			    {	        
			        context.write(key, new Text(value.toString().replace("_", " ")));
			    }
			    
			}
	
	public static class AbtPagerReducerOne extends Reducer<Text, Text, Text, Text> {
	    
		@Override
			    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
			    {
			    	String outlinks = "";
			    	Text out = new Text();
			    	Text url = new Text();
			    	
			    	for(Text t : values)
			    	{
			    		outlinks+=":"+t.toString().replaceAll("\\t+", "");
			    	}
			    	out.set(("1.0"+outlinks));
			    	url.set((key.toString()));
			    	context.write(url, out);
			    }
			    	  
			}
	
public static class AbtPagerMapperTwo extends Mapper<Text, Text, Text, Text> {
		
		@Override
			    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException, NullPointerException
			    {
				   context.write(key, value);
                   
				   Text did = new Text();
           	    	
           	       Text val = new Text();
           	
           	       String [] input = value.toString().split(":");
           	                			    	
           	     double currank = Double.parseDouble(input[0]);
		    	 double newrank = currank/(input.length-1);
		    	 
           			    	for(int t=1 ; t<input.length ; t++)
           			    	{
           			    		did.set(input[t]);
           			    		val.set(String.valueOf(newrank));
           			    		context.write(did, val);
           			    	}
           			    	
           			}
			    
			}

public static class AbtPagerReducerTwo extends Reducer<Text, Text, Text, Text> {
    
	private final double factor = 0.85;
	
	@Override
		    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		    {
		    	String outlinks="";
				double finalrank = 0.000000;
		    	
		    	for(Text t : values)
		    	{
		    	    String str = t.toString();
	                String[] star = str.split(":");
	                
	                if (star.length > 1) 
	                       outlinks = str.substring(str.indexOf(":"));
	                               
		    		finalrank+= Double.parseDouble(star[0]);
		    	}
		    	
		    	Text prank = new Text(((1-factor)+factor*finalrank) + outlinks);
				
		    	context.write(key, prank);
		    }
		    	  
		}
	
public static class AbtPagerMapperThree extends Mapper<Text, Text, Text, Text> {
	
	@Override
		    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException, NullPointerException
		    {	        Text rank = new Text(value.toString().split(":")[0]);
		        context.write(key, rank);
		    }
		    
		}

public static class AbtPagerReducerThree extends Reducer<Text, Text, Text, Text> {
    
	@Override
		    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		    {
				Text rank = new Text();
				
		    	for(Text t : values)
		    	{
		    		rank = t;
		    	}
		    	
		    	context.write(key,rank);
		    }
		    	  
		}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		 
			Configuration conf = new Configuration();
			
		    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
			Job job1 = new Job(conf, "Page Rank One");
			 
	        job1.setJarByClass(AbtPager.class);
	        
	        //job.setNumReduceTasks(1);
	        
	        job1.setOutputKeyClass(Text.class);
	        job1.setOutputValueClass(Text.class);

	        job1.setMapperClass(AbtPagerMapperOne.class);
	       
	        job1.setReducerClass(AbtPagerReducerOne.class);
	        
	        job1.setInputFormatClass(KeyValueTextInputFormat.class);
	        job1.setOutputFormatClass(TextOutputFormat.class);
	        
	        FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		    FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));

	        if(!job1.waitForCompletion(true))
	        	System.exit(0);
	        
	        int itr = 1;
	        String [] inputsecond = otherArgs;
	        
	        while(itr<11)
	        {
	        	if(itr==1)
	        	{	
	       	Job job2 = new Job(conf, "Page Rank Two");
			
			job2.setJarByClass(AbtPager.class);
	        
	        //job.setNumReduceTasks(1);
	        
	        job2.setOutputKeyClass(Text.class);
	        job2.setOutputValueClass(Text.class);

	        job2.setMapperClass(AbtPagerMapperTwo.class);
	       
	        job2.setReducerClass(AbtPagerReducerTwo.class);

	        job2.setInputFormatClass(KeyValueTextInputFormat.class);
	        job2.setOutputFormatClass(TextOutputFormat.class);

	        FileInputFormat.addInputPath(job2, new Path(inputsecond[1]+"/part-r-00000"));
		    FileOutputFormat.setOutputPath(job2, new Path(inputsecond[1]+"/Iteration "+itr));

	        if(!job2.waitForCompletion(true))
	        	System.exit(0);
	
	        itr++;
	        }
	        	else
	        	{
	        		Job job2 = new Job(conf, "Page Rank Two");
	    			
	    			job2.setJarByClass(AbtPager.class);
	    	        
	    	        //job.setNumReduceTasks(1);
	    	        
	    	        job2.setOutputKeyClass(Text.class);
	    	        job2.setOutputValueClass(Text.class);

	    	        job2.setMapperClass(AbtPagerMapperTwo.class);
	    	       
	    	        job2.setReducerClass(AbtPagerReducerTwo.class);

	    	        job2.setInputFormatClass(KeyValueTextInputFormat.class);
	    	        job2.setOutputFormatClass(TextOutputFormat.class);

	    	        FileInputFormat.addInputPath(job2, new Path(inputsecond[1]+"/Iteration "+(itr-1)+"/part-r-00000"));
	    		    FileOutputFormat.setOutputPath(job2, new Path(inputsecond[1]+"/Iteration "+itr));

	    	        if(!job2.waitForCompletion(true))
	    	        	System.exit(0);
	    	
	    	        itr++;
	        	}
	        
	        }
	        
	        Job job3 = new Job(conf, "Page Rank Three");
			 
	        job3.setJarByClass(AbtPager.class);
	        
	        //job.setNumReduceTasks(1);
	        
	        job3.setOutputKeyClass(Text.class);
	        job3.setOutputValueClass(Text.class);

	        job3.setMapperClass(AbtPagerMapperThree.class);
	       
	        job3.setReducerClass(AbtPagerReducerThree.class);
	        
	        job3.setInputFormatClass(KeyValueTextInputFormat.class);
	        job3.setOutputFormatClass(TextOutputFormat.class);
	        
	        FileInputFormat.addInputPath(job3, new Path(inputsecond[1]+"/Iteration "+(itr-1)+"/part-r-00000"));
		    FileOutputFormat.setOutputPath(job3, new Path(otherArgs[1]+"/Result"));

	        if(!job3.waitForCompletion(true))
	        	System.exit(0);
	}

}
