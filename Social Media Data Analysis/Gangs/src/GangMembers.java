import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class GangMembers {

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
	    		  context.write(key, new Text("#"+value.toString()));
	    	   }
	       }
	       
	}
	
	public static class ReduceCrime extends Reducer<Text, Text, Text, Text> {
		
		private HashMap<String, Integer> NumCrimes = new HashMap<String, Integer>();
		private HashMap <String, HashMap<String, Integer>> Accomplices = new HashMap <String, HashMap<String, Integer>>();
		private HashMap <String, Set<String>> Friends = new HashMap <String, Set<String>>();
		
		public void reduce(Text key, Iterable<Text> values, Context context) 
				 throws IOException, InterruptedException, NullPointerException 
				 {
			
			String friends = "";
			boolean flag = false;
			
			for(Text t : values)
			{
				if(t.toString().contains("#"))
				{
					flag=true;
					String [] r = t.toString().replaceAll("#", ":").split(":");
					
					if(NumCrimes.containsKey(key.toString()))
						NumCrimes.put(key.toString(), NumCrimes.get(key.toString())+1);
					else
						NumCrimes.put(key.toString(), 1);
					
					for(String a : r)
					{
						if(!a.isEmpty())
						{
						HashMap<String, Integer> acmplc = new HashMap<String, Integer>();
						
						if(Accomplices.containsKey(key.toString()))
						{
							acmplc = Accomplices.get(key.toString());
							if(acmplc.containsKey(a))
								acmplc.put(a, acmplc.get(a)+1);
							else
								acmplc.put(a, 1);
							
							Accomplices.put(key.toString(), acmplc);
						}
						else
						{
							acmplc.put(a,1);
							Accomplices.put(key.toString(), acmplc);
						}
					}	
					
				}
					
				}
				else
				friends+=t.toString()+",";
			}
			
			if(flag)
			{
				String [] frnds = friends.split(",");
				for(String f : frnds)
					if(Friends.containsKey(key.toString()))
						Friends.get(key.toString()).add(f);
					else
						{
						Friends.put(key.toString(), new HashSet<String>());
						Friends.get(key.toString()).add(f);
						}
			}
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException
		{
			HashMap <String, Set<String>> Gangs = new HashMap <String, Set<String>>();
			
			for (Map.Entry<String, HashMap<String, Integer>> prep : Accomplices.entrySet())
			{
				HashMap<String, Integer> suspects = prep.getValue();
				
					for(Map.Entry<String, Integer> inner : suspects.entrySet())
					{
						if(inner.getValue()>=2 && NumCrimes.get(inner.getKey())>=3 && Friends.get(prep.getKey()).contains(inner.getKey()))
						{
							if(Gangs.containsKey(prep.getKey()))
								Gangs.get(prep.getKey()).add(inner.getKey());
							else
							{
								Gangs.put(prep.getKey(), new HashSet<String>());
								Gangs.get(prep.getKey()).add(inner.getKey());
							}
							
							if(Gangs.containsKey(inner.getKey()))
								Gangs.get(inner.getKey()).add(prep.getKey());
							else
							{
								Gangs.put(inner.getKey(), new HashSet<String>());
								Gangs.get(inner.getKey()).add(prep.getKey());
							}
						}
					}
			}
			/*for (Map.Entry<String, Set<String>> prep : Gangs.entrySet())
			{
				System.out.println("Crook "+prep.getKey());
				
				for(String p : prep.getValue())
				{
					System.out.print(p+" ");
				}
			}*/
			ArrayList <Set<String>> ganGmemberS = new ArrayList<Set<String>>();
			
			for(Map.Entry<String, Set<String>> en : Friends.entrySet())
			{
				
				Set<String> gm = new HashSet<String>();
				gm.add(en.getKey());
				for(String sus : en.getValue())
				{
					
					if(Gangs.get(en.getKey()).contains(sus))
						{
						gm.add(sus);
							if(Gangs.containsKey(sus))
							{
								for(String g : Gangs.get(sus))
									gm.add(g);
							}
						}
				}
				if(gm.size()>=5 && !ganGmemberS.contains(gm))
					ganGmemberS.add(gm);
			}
			
			for(Set<String> st : ganGmemberS)
			{
				
				String list="";
				
				for(String l : st)
					 list+=l+" ";
				
				context.write(new Text("Gang"), new Text(list));
			}
			
		}
		
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		 
		 String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		 Job job = new Job(conf, "Friends");
		       
		 	   job.setJarByClass(GangMembers.class);
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
		       
		 	   job2.setJarByClass(GangMembers.class);
		       
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
