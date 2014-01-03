import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.util.*;

public class Join {

	public static class Map extends Mapper<LongWritable, Text, Text,Text> {
		  
		       private Text jkey = new Text();
		       private Text vals = new Text();
		           
		       public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException, NullPointerException {
		           String line = value.toString();
		           
		           String first,second, third;
		           
		           first=line.substring(0, line.indexOf(','));
		           
		           second = line.substring(line.indexOf(',')+1, line.lastIndexOf(','));
		           
		           third = line.substring(line.lastIndexOf(',')+1);
		           
		           StringBuffer val = new StringBuffer();
		           
		           if (first.matches("-?\\d+(\\.\\d+)?"))
		           {
		        	   jkey.set(third);
		        	   val.append(first);
			           val.append(",");
			           val.append(second);
			           
		           }
		           else
		           {
		        	   jkey.set(first);
		        	   val.append(second);
			           val.append(",");
			           val.append(third);
		           }
		           vals.set(val.toString());
		           
		           context.write(jkey, vals);
		         
		       }
		    } 
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		HashMap<String, HashMap<String, ArrayList<String>>> join = new HashMap<String, HashMap<String, ArrayList<String>>>();
		
		  
		public void reduce(Text key, Iterable<Text> values, Context context) 
				 throws IOException, InterruptedException, NullPointerException {
		
			String line;
			
			ArrayList<String> fields = new ArrayList<String>();
			
			Text val = new Text();
			String name="";
			
			ArrayList<String> tid = new ArrayList<String>();
			ArrayList<String> tweet = new ArrayList<String>();
			
			String state="";
			
			for(Text v : values)
				{
				
				line=v.toString();
				String first,second;
		           
		        first=line.substring(0, line.indexOf(','));
		           
		        second = line.substring(line.indexOf(',')+1);
		        if(first.matches("-?\\d+(\\.\\d+)?"))
					{tid.add(first);tweet.add(second);}
				else 
				{	name=first;state=second;  }
		        fields.add(first);
		        fields.add(second);
		            
				}
			
			Iterator<String> tidit = tid.iterator();
			Iterator<String> tweetit = tweet.iterator();
			
			while (tidit.hasNext() && tweetit.hasNext())
			{	
				StringBuffer finalval = new StringBuffer();
				finalval.append("|");
				finalval.append(name);
				finalval.append("|");
				
				finalval.append(tweetit.next());
				
				finalval.append("|");
				
				finalval.append(tidit.next());
				
				finalval.append("|");
				finalval.append(state);
				
				val.set(finalval.toString());
			
				context.write(key, val);
			}
			
		}
		
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, NullPointerException {
		Configuration conf = new Configuration();
		 
		 String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		 if (otherArgs.length < 6) {
		      System.err.println("Usage: ReduceSideJoin -input1 <file name> -input2 <file name> -output <file name>");
		      System.exit(2);
		    }  
		           
		 Job job = new Job(conf, "ReduceSideJoin");
		       
		 	   job.setJarByClass(Join.class);
		       job.setOutputKeyClass(Text.class);
		       job.setOutputValueClass(Text.class);
		           
		       job.setMapperClass(Map.class);
		       job.setReducerClass(Reduce.class);
		       if(otherArgs.length==7)
		       {
		    	   MultipleInputs.addInputPath(job, new Path(otherArgs[2]), TextInputFormat.class);
		           MultipleInputs.addInputPath(job, new Path(otherArgs[4]), TextInputFormat.class);
		     
		       job.setOutputFormatClass(TextOutputFormat.class);
		           
		       FileOutputFormat.setOutputPath(job, new Path(otherArgs[6]));
		       }
		       if(otherArgs.length==6)
		       {
		    	   MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class);
		           MultipleInputs.addInputPath(job, new Path(otherArgs[3]), TextInputFormat.class);
		      
		       job.setOutputFormatClass(TextOutputFormat.class);
		           
		       FileOutputFormat.setOutputPath(job, new Path(otherArgs[5]));
		       }
		       System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
