import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

public class PairsMain {
	
	public static class WordPair implements WritableComparable<WordPair>{
		
		String Word = "";
		String Neighbor = "";
		
		public void setWord(String w)
		{
			Word = w;
		}
		
		public void setNeighbor(String n)
		{
			Neighbor = n;
		}
		
		public String getWord()
		{
			return Word;
		}
		
		public String getNeighbor()
		{
			return Neighbor;
		}
		
		@Override
		public void write(DataOutput out) throws IOException {
			
			out.writeBytes(Word);
			out.writeBytes(" ");
			out.writeBytes(Neighbor);
		}
		
		@Override
		public void readFields(DataInput in) throws IOException {
			
			String [] input = in.readLine().trim().split(" ");
			Word = input[0]; 
	        Neighbor = input[1];
	     }
				
		@Override
		public int compareTo(WordPair other) throws NullPointerException{
		
			String neigh = "", wd="";
			
			wd=this.Word;
			
			neigh=this.Neighbor;
			
			int returnVal = wd.compareTo(other.getWord());
			
			if(returnVal != 0)
			{
				        return returnVal;
			}
				    if(neigh.equals('*'))
				    {
				        return -1;
				    }
				    else if(other.getNeighbor().equals('*'))
				    {
				        return 1;
				    }
			    
				    return this.Neighbor.compareTo(other.getNeighbor());
				}

		
		
	}
	
	public static class PairsRelativeOccurrenceMapper extends Mapper<LongWritable, Text, WordPair, IntWritable> {
		
		private IntWritable ONE = new IntWritable(1);
		
		@Override
			    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException, NullPointerException{
			        
			    	String[] tokens = value.toString().split("\\W+");
			    	//System.out.println("^ "+value.toString());
			        WordPair wordPair = new WordPair();
			        HashMap <String, Integer> tots = new HashMap<String,Integer>();  
			        	for (int i = 0; i < tokens.length-1; i++) 
			        	{
			                    tokens[i] = tokens[i].replaceAll("\\W+","");
			                    
			                    if(tokens[i].equals("")){
			                        continue;
			                    }
			 
			                    
			                    wordPair.setWord(tokens[i]);
			                   
			                    if(!tots.containsKey(tokens[i]))
			                    {
			                    	tots.put(tokens[i], 1);
			                    	
			                    }
			                    else
			                    	tots.put(tokens[i],tots.get(tokens[i])+ 1);
			                    
			                        wordPair.setNeighbor(tokens[i+1].replaceAll("\\W+", ""));
			                        //System.out.println(wordPair.getWord()+" "+wordPair.getNeighbor());
			                        context.write(wordPair, ONE);
			                    }
			        	
			        for (Map.Entry<String, Integer> en : tots.entrySet())
			        {
			        	WordPair wp = new WordPair();
			            IntWritable totalCount = new IntWritable();
			        	wp.setWord(en.getKey());
			        	wp.setNeighbor("*");
			        	//System.out.println(wp.getWord()+" "+wp.getNeighbor()+"  "+en.getValue());
	                    totalCount.set(en.getValue());
	                    context.write(wp, totalCount);
			        	
			        }
			        
			    }
			    
			}
		
		public static class WordPairPartitioner extends Partitioner<WordPair,IntWritable> {
			 
			    @Override
			    public int getPartition(WordPair wordPair, IntWritable intWritable, int numPartitions) {
			        return wordPair.getWord().hashCode() % numPartitions;
			    }
			}
	
	public static class PairsRelativeOccurrenceReducer extends Reducer<WordPair, IntWritable, Text, DoubleWritable> {
			    
		private HashMap<String, Integer> tots = new HashMap<String, Integer>();
		private HashMap<String, HashMap< String, Integer>> bigtots = new HashMap<String, HashMap< String, Integer>>();
		
		@Override
			    protected void reduce(WordPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
			    {
			    	
			    	if(key.getNeighbor().equalsIgnoreCase("*"))
			    	{
			    		//System.out.println(key.getWord()+" "+key.getNeighbor());
			    		if(!tots.containsKey(key.getWord()))
			    			tots.put(key.getWord(), 0);
			    		
			    	for (IntWritable value : values) 
			    	{
			    		//System.out.println(value.get());
			    		tots.put(key.getWord(), tots.get(key.getWord())+value.get());
			        }
			    }
			    	else
			    	{
			    		if(!bigtots.containsKey(key.getWord()))
			    			bigtots.put(key.getWord(), new HashMap<String, Integer>());
			    		
			    		HashMap<String, Integer> counter = bigtots.get(key.getWord());
			    		
			    		for (IntWritable value : values) 
			    		{	    			
			    			if(!counter.containsKey(key.getNeighbor()))
				    			counter.put(key.getNeighbor(), 1);
			    			else
			    				counter.put(key.getNeighbor(), counter.get(key.getNeighbor())+value.get());
			    			/*//System.out.println(key.getWord()+" "+key.getNeighbor());
			    			//System.out.println(value.get());
			    			Text bigram = new Text();
			    		    DoubleWritable relativeCount = new DoubleWritable();
			    			bigram.set(key.getWord()+" "+key.getNeighbor());
			    			double relfreq = (double) value.get() / tots.get(key.getWord());
			    			//System.out.println(key.getWord()+" "+key.getNeighbor()+" "+relfreq);
			    			relativeCount.set(relfreq);
			    			context.write(bigram, relativeCount);*/
			    		}
			    		
			    	}
			    	
			    }
			    
			   protected void cleanup(Context context) throws IOException, InterruptedException
			   {
				    
				   for(Map.Entry<String, HashMap< String, Integer>> en : bigtots.entrySet())
				   {
					    Text bigram = new Text();
		    		    DoubleWritable relativeCount = new DoubleWritable();
		    		    
		    		    HashMap<String, Integer> counter = bigtots.get(en.getKey());
		    		    
		    		    for(Map.Entry<String, Integer> inner : counter.entrySet())
		    		    {
		    		    	bigram.set(en.getKey()+" "+inner.getKey());
		    		    	double relfreq = (double) inner.getValue() / tots.get(en.getKey());
		    		    	relativeCount.set(relfreq);
		    		    	context.write(bigram, relativeCount);
		    		    }
		    		}
			   }
			}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		 Configuration conf = new Configuration();
		 
		    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
			Job job = new Job(conf, "Pairs");
			 
	        job.setJarByClass(PairsMain.class);
	        
	        //job.setNumReduceTasks(1);
	        
	        job.setOutputKeyClass(WordPair.class);
	        job.setOutputValueClass(IntWritable.class);

	        job.setMapperClass(PairsRelativeOccurrenceMapper.class);
	       
	        job.setReducerClass(PairsRelativeOccurrenceReducer.class);

	        job.setPartitionerClass(WordPairPartitioner.class);
	        
	        job.setInputFormatClass(TextInputFormat.class);
	        job.setOutputFormatClass(TextOutputFormat.class);

	        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

	        job.waitForCompletion(true);

	}

}
