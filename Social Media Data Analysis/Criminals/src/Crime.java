import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Crime {
	
	public static class MapFriend extends Mapper<Text, Text, Text,Text> {
		 	           
	       public void map(Text key, Text value, Context context) throws IOException, InterruptedException, NullPointerException {

	    	   context.write(key, value);
	    	   context.write(value,key);
	       }
	       
	}
	
	public static class ReduceFriend extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context) 
				 throws IOException, InterruptedException, NullPointerException {
			
			String friends = "";
			
			for(Text t : values)
			
				friends+=t.toString()+",";
			
			context.write(new Text(key.toString()), new Text(friends));
		
		}
		
	}
	
	public static class MapCrime extends Mapper<Text, Text, Text,Text> {
         
	       public void map(Text key, Text value, Context context) throws IOException, InterruptedException, NullPointerException {
	    	   
	    	   if(!value.toString().contains(":"))
	    	   {
	    		   String fr [] = value.toString().split(",");
	    		   for (String j : fr)
	    			   context.write(new Text(j),key);
	    	   }
	    	   else
	    	   {
	    		  context.write(key, new Text("^"));
	    	   }
	       }
	       
	}
	
	public static class ReduceCrime extends Reducer<Text, Text, Text, Text> {
		
		private HashMap<String, Integer> ARY = new HashMap<String, Integer>();
		
		public void reduce(Text key, Iterable<Text> values, Context context) 
				 throws IOException, InterruptedException, NullPointerException 
				 {
			
			String friends = "";
			
			for(Text t : values)
			
				friends+=t.toString()+",";
			
			if(friends.contains("^"))
			{
				
				String risk [] = friends.split(",");
				
				for (String r : risk)
				{
					if(!r.contains("^"))
					if(ARY.containsKey(r))
						ARY.put(r, ARY.get(r)+1);
					else
						ARY.put(r, 1);
				}
			}
		
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException
		{
			for(Map.Entry<String, Integer> en : ARY.entrySet())
			{
				if(en.getValue()>=2)
					context.write(new Text(en.getKey()), new Text("is at risk !"));
			}
		}
		
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
	{
		Configuration conf = new Configuration();
		 
		 String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		 Job job = new Job(conf, "Friends");
		       
		 	   job.setJarByClass(Crime.class);
		       job.setOutputKeyClass(Text.class);
		       job.setOutputValueClass(Text.class);
		           
		       job.setMapperClass(MapFriend.class);
		       job.setReducerClass(ReduceFriend.class);
		       
		       job.setInputFormatClass(KeyValueTextInputFormat.class);
		       job.setOutputFormatClass(TextOutputFormat.class);
		        
		       FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			   FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
			   
			   if(!job.waitForCompletion(true))
		        	System.exit(0);       
		       
			   Job job2 = new Job(conf, "Crime Record");
		       
		 	   job2.setJarByClass(Crime.class);
		       
		 	   job2.setOutputKeyClass(Text.class);
		       job2.setOutputValueClass(Text.class);
		           
		       job2.setMapperClass(MapCrime.class);
		       job2.setReducerClass(ReduceCrime.class);
		       
		       job2.setInputFormatClass(KeyValueTextInputFormat.class);
		       job2.setOutputFormatClass(TextOutputFormat.class);
		        
		       MultipleInputs.addInputPath(job2, new Path(otherArgs[1]), KeyValueTextInputFormat.class);
		       MultipleInputs.addInputPath(job2, new Path(otherArgs[2]+"/part-r-00000"), KeyValueTextInputFormat.class);
			   
		       FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]+"/risk"));
		      		       
		       System.exit(job2.waitForCompletion(true) ? 0 : 1);
		       
		      

	}

}
