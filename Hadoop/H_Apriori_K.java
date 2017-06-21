package frequent_items_finding;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import document_similarity.Document_NN_Query_Combination_Sort;
import document_similarity.Document_NN_Query_Combination_Sort.SortMapper;
import document_similarity.Document_NN_Query_Combination_Sort.SortReducer;

import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.HashMap;


public class H_Apriori_K {
	
	//private final static double  threshold=3000; 
	
	
	public static boolean share_terms(String itemset1, String itemset2)
	{
		String[] itemset1_list=itemset1.split("[ \t]");
		String[] itemset2_list=itemset2.split("[ \t]");
		int length_of_itemset=itemset1_list.length;
		//doc_left_list.
		int ct=0;
		for(int i=0;i<length_of_itemset;i++)
		{
			//ct=0;
			int j=0;
			for(;j<length_of_itemset;j++)
			{
				if(itemset1_list[i].equalsIgnoreCase(itemset2_list[j]))
				{
					break;
				}
				
			}
			if(j==length_of_itemset)
				{
					ct++;
				}
		}
		
		
		if(ct==1)
		{
			return true;
		}else
		{
			return false;
		}
	}
	
	private static String term_combination(String doc_left,String doc_right)
	{
		String[] doc_left_list=doc_left.split("[ \t]");
		String[] doc_right_list=doc_right.split("[ \t]");
		String tmp="";
		int length_of_doc_left_list=doc_left_list.length;
		int length_of_doc_right_list=doc_right_list.length;
		int i=0;
		int j=0;
		while(i<length_of_doc_left_list&&j<length_of_doc_right_list)
		{
			//int cur_doc_left=Integer.parseInt(doc_left_list[i].substring(3));
			//int cur_doc_right=Integer.parseInt(doc_right_list[j].substring(3));
			
			int cur_doc_left=Integer.parseInt(doc_left_list[i]);
			int cur_doc_right=Integer.parseInt(doc_right_list[j]);
			if(cur_doc_left<cur_doc_right)
			{
				tmp+=doc_left_list[i]+" ";
				i++;
			}else if(cur_doc_left==cur_doc_right)
			{
				tmp+=doc_left_list[i]+" ";
				i++;
				j++;
			}
			else{
				tmp+=doc_right_list[j]+" ";
				j++;
			}
		}
		
		if(i==length_of_doc_left_list){
			for(;j<length_of_doc_right_list;j++)
			{
				tmp+=doc_right_list[j]+" ";
				j++;
			}
		}else{
			for(;i<length_of_doc_left_list;i++)
			{
				tmp+=doc_left_list[i]+" ";
				i++;
			}
		}
		
		return tmp.trim();
	}
	

	
	  public static class pass1Mapper extends Mapper<Object, Text, Text, Text>{
	    
	    /*private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();*/
	    //private final static int k=4;  //输入分块个数
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    	
	    	String[] items=value.toString().split("[ \t]");
	    	int length_of_items=items.length;
	    	for(int i=1;i<length_of_items;i++)
	    	{
	    		context.write(new Text(items[i]), new Text(items[0]));
	    	}
	    	
	    	
	    }
	  }
	  
	  public static class pass1Reducer extends Reducer<Text,Text,Text,Text> {
	    //private IntWritable result = new IntWritable();
	    
	    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
	      
	    	
	    	int threshold=Integer.parseInt(context.getConfiguration().get("threshold"));
	    	//StringBuilder tids=",";//用逗号分割键和值
	    	StringBuilder tids=new StringBuilder(",");
	    	int counter=0;
	    	for(Text val: values)
	    	{
	    		tids.append(val.toString()+" ");
	    		//tids=tids+val.toString()+" ";
	    		counter++;
	    	}
	    	
	    	if(counter>=threshold)
	    	{
	    		//System.out.println("pass1reduce "+key.toString()+tids.toString());
	    		context.write(key, new Text(tids.toString()));
	    		//context.write(key, new Text(tids.toString().trim()));
	    	}
	    	
	    }
	    
	  }//term, tidlist 104,6 14 
	  
	  
	  
	  
	  public static class recoverMapper extends Mapper<Object, Text, Text, Text>{
		    
		    /*private final static IntWritable one = new IntWritable(1);
		    private Text word = new Text();*/
		    //private final static int k=4;  //输入分块个数
		    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		    	String[] key_value=value.toString().split(",");
		    	String[] tids=key_value[1].toString().trim().split(" ");
		    	int length_of_items=tids.length;
		    	for(int i=0;i<length_of_items;i++)
		    	{
		    		context.write(new Text(tids[i]), new Text(key_value[0].trim()));//
		    	}
		    	
		    	
		    }
		  }
		  
		  public static class recoverReducer extends Reducer<Text,Text,Text,Text> {
		    //private IntWritable result = new IntWritable();
		    
		    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		      
		    	
		    	//int threshold=Integer.parseInt(context.getConfiguration().get("threshold"));
		    	int k=Integer.parseInt(context.getConfiguration().get("k"));
		    	//StringBuilder terms=",";
		    	StringBuilder terms=new StringBuilder(",");
		    	HashSet<String> terms_unique=new HashSet<String>();
		    	HashSet<String> itermset_set=new HashSet<String>();
		    	//int counter=0;
		    	for(Text val: values)
		    	{
		    		//if(itermset_set.add(val.toString())){
		    			terms.append(val.toString()+"|");
		    			
		    			String[] itermset=val.toString().split(" ");
			    		int length_of_itermset=itermset.length;
			    		for(int i=0;i<length_of_itermset;i++)
			    		{
			    			terms_unique.add(itermset[i]);
			    		}
		    		//}
		    		
		    		
		    		//terms=terms+val.toString()+" ";
		    		//counter++;
		    	}
		    	
		    	
		    	
		    	if(terms_unique.size()>=k)
		    	{
		    		
		    		context.write(key, new Text(terms.toString()));
		    	}
		    	//System.out.println("recover reduce"+key.toString()+terms.toString());
		    	//context.write(key, new Text(terms.toString()));
		    	
		    	/*if(counter>threshold)
		    	{
		    		context.write(key, new Text(terms));
		    	}*/
		    	
		    }
		    
		  }//tid, term list|||   1,274|834|825|775|730|25|52|
		  
		  public static class passiMapper extends Mapper<Object, Text, Text, Text>{
			    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			    	String[] key_value=value.toString().trim().split(",");
			    	String[] items=key_value[1].toString().trim().split("\\|");//多了一个空格！！！！
			    	int length_of_items=items.length;
			    	
			    	
			    	for(int i=0;i<length_of_items;i++)
			    	{
			    		for(int j=i+1;j<length_of_items;j++)
			    		{
				    		if(share_terms(items[i].toString(),items[j].toString()))
				    		{
				    			//term_combination(items[i].toString(),items[i+1].toString());
				    			context.write(new Text(term_combination(items[i].toString(),items[j].toString())),new Text(key_value[0].trim()));
				    		}
			    		}
			    		//context.write(new Text(items[i]), new Text(items[0]));//
			    	}
			    	
			    }
			  }//items tid
			  
			  public static class passiReducer extends Reducer<Text,Text,Text,Text> {
			    //private IntWritable result = new IntWritable();
			    
			    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			      
			    	
			    	
			    	int threshold=Integer.parseInt(context.getConfiguration().get("threshold"));
			    	//String terms=",";
			    	StringBuilder tids=new StringBuilder(",");
			    	HashSet<String> tmp_tids=new HashSet<String>();
			    	
			    	
			    	int counter=0;
			    	for(Text val: values)
			    	{
			    		if(tmp_tids.add(val.toString()))
			    		{
			    			tids.append(val.toString()+" ");
			    			counter++;
			    		}
			    		
			    		//tids.append(val.toString()+" ");
			    		//terms=terms+val.toString()+" ";
			    		
			    	}
			    	
			    	/*Iterator<String> it = tmp_tids.iterator();
			    	while (it.hasNext()) {
			    	  //String str = it.next();
			    	  tids.append(it.next().toString()+" ");
			    	  counter++;
			    	  //System.out.println(str);
			    	}*/
			    	
			    	if(counter>=threshold)
			    	{
			    		//System.out.println("passireduce  "+key.toString()+tids.toString());
			    		context.write(key, new Text(tids.toString()));
			    	}
			    	
			    }
			    
			  }//terms, tid list  208 279,16 9
		  
		  
	  
	  
	  

	  public static void main(String[] args) throws Exception {
		long starttime=System.nanoTime();
	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 4) {
	      System.err.println("Usage: H_Apriori <in> <out> iteration threshold");
	      System.exit(2);
	    }
	    
	    conf.set("threshold", args[3]);
	    Job job = new Job(conf, "SAX_HDSJ");
	    
	    job.setJarByClass(H_Apriori_K.class);
	    
	    job.setMapperClass(pass1Mapper.class);
	    //job.setCombinerClass(IntSumReducer.class);
	   
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setReducerClass(pass1Reducer.class);
	    //FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    //FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1].toString()+"/frequent/1"));
	    int pass_of_iteration=Integer.parseInt(args[2].toString());
	    
	    long endtime=0;
	    if(job.waitForCompletion(true))
	    {
	    	endtime=System.nanoTime();
	    }
	    
	    
	    for(int i_u=0;i_u<pass_of_iteration;i_u++)
	    {
	    	Configuration conf3=new Configuration();
	    	conf3.set("threshold", args[3]);
	    	conf3.set("k", args[2]);
	    	Job job3 = new Job(conf3,"recover");
	        job3.setJarByClass(H_Apriori_K.class);
	        //FileInputFormat.addInputPath(job3, new Path("/user/hdfs/lpp/out_2/part*"));
	        FileInputFormat.addInputPath(job3, new Path(args[1].toString()+"/frequent/"+(i_u+1)+"/part*"));
	        FileOutputFormat.setOutputPath(job3, new Path(args[1].toString()+"/recover/"+i_u));
	        job3.setMapperClass(recoverMapper.class);
	        job3.setReducerClass(recoverReducer.class);
	        job3.setMapOutputKeyClass(Text.class);
	        job3.setMapOutputValueClass(Text.class);
	        job3.waitForCompletion(true);
	        
	        
	        
	        Configuration conf4=new Configuration();
	        conf4.set("threshold", args[3]);
	    	Job job4 = new Job(conf4,"frequent");
	        job4.setJarByClass(H_Apriori_K.class);
	        //FileInputFormat.addInputPath(job3, new Path("/user/hdfs/lpp/out_2/part*"));
	        FileInputFormat.addInputPath(job4, new Path(args[1].toString()+"/recover/"+i_u+"/part*"));
	        FileOutputFormat.setOutputPath(job4, new Path(args[1].toString()+"/frequent/"+(i_u+2)));
	        job4.setMapperClass(passiMapper.class);
	        job4.setReducerClass(passiReducer.class);
	        job4.setMapOutputKeyClass(Text.class);
	        job4.setMapOutputValueClass(Text.class);
	        job4.waitForCompletion(true);
	        
	        
	        
	    }
	    
	    //System.out.println("time cost is "+(endtime-starttime));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
	

}
