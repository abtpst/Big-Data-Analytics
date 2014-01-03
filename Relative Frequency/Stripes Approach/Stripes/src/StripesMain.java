
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.TreeSet;

public class StripesMain {
    
    public static class Map extends Mapper<LongWritable, Text, Text, MapWritable> {
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
	    	String[] inp = value.toString().split(" ");
    		
    		String u;
    		String v;
    		HashMap<String, HashMap<String, Integer>> fintots = new HashMap<String,HashMap<String, Integer>>();

    		for (int y = 0 ; y < inp.length-1 ; y++)
    		{
    			u=inp[y];
    			v=inp[y+1];
    			
    			if(u.matches("^\\w+$") && v.matches("^\\w+$"))
    			{
    				if(!fintots.containsKey(u))
    				{
    					HashMap<String, Integer> utots = new HashMap<String, Integer>();
    					utots.put(v, 1);
    					fintots.put(u, utots);
    				}
    				else
    				{
    					HashMap<String, Integer> utots = fintots.get(u);
    					
    					if(!utots.containsKey(v))
    						utots.put(v, 1);
    					else
    						utots.put(v, utots.get(v)+1);
    					
    					fintots.put(u, utots);
    				}
    			}
    		}	
    				
    		for(Entry<String, HashMap<String, Integer>> entry : fintots.entrySet())
    		{
    			Text jkey = new Text();
    		    MapWritable jvals = new MapWritable();
    			
    		    HashMap<String, Integer> utots = entry.getValue();
    			
    			jkey.set(entry.getKey());
    			
    			for(Entry<String, Integer> inner : utots.entrySet())
    			{
    				Text uword = new Text();
    	    		IntWritable count = new IntWritable();
    				uword.set(inner.getKey());
    				count.set(inner.getValue());
    				jvals.put(uword, count);
    			}
    			
    			context.write(jkey, jvals);
    		}
	        	   
	    }
	 }

   public static class Reduce extends Reducer<Text, MapWritable, Text, DoubleWritable> {
        
    	@SuppressWarnings("null")
		public void reduce(Text key, Iterable<MapWritable> values, Context context)
                throws IOException, InterruptedException {
        	
    		int totalCount = 0;
            
            HashMap<String, Integer> tots = new HashMap<String,Integer>();
            
            for (MapWritable val : values) 
            {
            	
            	for (Entry<Writable,Writable> v : val.entrySet())
            	{
            		String uw = v.getKey().toString();
            		int count = Integer.parseInt(v.getValue().toString());
            		
            		if(tots.containsKey(uw))
            			tots.put(uw, tots.get(uw)+count);
            		else
            			tots.put(uw, count);
            		
            	}
            }
          
            for (Entry<String, Integer> entry : tots.entrySet()) 
            {
            	totalCount+=entry.getValue();
            }
            
            double relfreq = 0.00000000000000;
            String keyval = "";
            
            for (Entry<String, Integer> entry : tots.entrySet()) 
            {
            	Text rfkey = new Text();
            	DoubleWritable rfval = new DoubleWritable();
            	relfreq = (double)(entry.getValue())/(double)(totalCount);
            	
            	keyval = key+" "+entry.getKey();
            	
            	rfkey.set(keyval);
            	rfval.set(relfreq);
            	
            	context.write(rfkey, rfval);
            	            	
            }
            
        }

     }
    
public static void main(String[] args) throws Exception {
        
	    Configuration conf = new Configuration();
	 
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	
		Job job = new Job(conf, "Stripes");
		 
        job.setJarByClass(StripesMain.class);
        
        job.setNumReduceTasks(1);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);

        job.setMapperClass(Map.class);
       
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.waitForCompletion(true);
    }

}