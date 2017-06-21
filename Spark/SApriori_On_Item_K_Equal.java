package frequent_items_finding;
import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.HashMap;
import java.util.HashSet;

public class SApriori_On_Item_K_Equal {
	
	
	
	//private static int threshold=250;
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
	
	private static boolean share(String doc_left,String doc_right)
	{
		String[] doc_left_list=doc_left.split("[ \t]");
		String[] doc_right_list=doc_right.split("[ \t]");
		int length_of_doc_list=doc_left_list.length;
		//doc_left_list.
		int ct=0;
		for(int i=0;i<length_of_doc_list;i++)
		{
			//ct=0;
			int j=0;
			for(;j<length_of_doc_list;j++)
			{
				if(doc_left_list[i].equalsIgnoreCase(doc_right_list[j]))
				{
					break;
				}
				
			}
			if(j==length_of_doc_list)
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
	
	
	public static void main (String[] args) throws Exception
	{
		if(args.length!=3)// we want to get parameter from the commond line, however yarn-cluster shield the screen output, then we determine the threshold with a static parameter
		{
			System.out.println("please determined the target");
			return;
		}
		//SparkConf conf=new SparkConf().setAppName("spark max frequent items").setMaster("yarn-cluster"); 
		SparkConf conf=new SparkConf().setAppName("spark "+args[0]).setMaster("yarn-cluster"); 
		JavaSparkContext sc=new JavaSparkContext(conf); 
		
		JavaRDD<String> lines = sc.textFile("hdfs://master:8020"+args[0]);
		final int iteration=Integer.parseInt(args[1]);
		final int  threshold=Integer.parseInt(args[2]);
		//JavaRDD<String> lines = sc.textFile("hdfs://master:8020/home/3.txt");
		//JavaRDD<String> t = sc.textFile("hdfs://master:8020/home/twitter.txt");
		 
		
		JavaRDD<Tuple2<String,String>> words = lines.flatMap(new FlatMapFunction<String,Tuple2<String,String>>() {
		      @Override
		      public Iterable<Tuple2<String,String>> call(String s) {
		        ArrayList<Tuple2<String,String>> terms_in_document=new ArrayList<Tuple2<String,String>>();
		        HashSet<String> hs=new HashSet<String>();
		        String[] terms=s.split("[ \t]");
		        
		        int length_of_document=terms.length;
		        for(int i=1;i<length_of_document;i++)//the first is documentid
		        {
		        	/*int pre_length_hs=hs.size();
		        	hs.add(terms[i]);
		        	int aft_length_hs=hs.size();
		        	if(pre_length_hs<aft_length_hs)// in order to ensure that all words appear only once
		        	{*/
		        		terms_in_document.add(new Tuple2<String,String>(terms[i],terms[0]));
		        	/*}*/
		        }
		        return terms_in_document;
		      }
		    });// <term,docid>
		JavaPairRDD<String,String> term_doc=words.mapToPair(new PairFunction<Tuple2<String,String>,String,String>(){
			@Override
			public Tuple2<String,String> call(Tuple2<String,String> t)
			{
				return new Tuple2<String,String>(t._1,t._2);
			}
		});//<term,docid>
		
		
		
		JavaPairRDD<String,String> terms_docs=term_doc.reduceByKey(new Function2<String,String,String>(){
			@Override
			public String call(String s1,String s2)
			{
				return s1+","+s2;
			}
		});//<term,docid1,docid2>
		
		
		long counter=-1;
		long cur_counter=0;
		
		
		JavaPairRDD<String,String> terms_docs_filtered=terms_docs.filter(new Function<Tuple2<String,String>,Boolean>()
				{
					@Override
			 		public Boolean call(Tuple2<String,String> t) {
						return t._2.split(",").length>=threshold;
			 			//return s.contains("MySQL"); 
			 			}
			
				}).cache();
		
		//获得了频繁一项集
		
		//terms_docs_filtered.saveAsTextFile("hdfs://master:8020/user/hdfs/lpp/Apriori_On_Item/"+5);
		for(int i=0;i<iteration;i++){
		//for(int i=0;i<3;i++){//this will determined the length of frequent itemsets
			////terms_docs  马鞍山，doc1,doc2,doc3
		JavaRDD<Tuple2<String,String>> term_doc_composition=terms_docs_filtered.flatMap(new FlatMapFunction<Tuple2<String,String>,Tuple2<String,String>>(){
		//term_doc_composition=terms_docs.flatMap(new FlatMapFunction<Tuple2<String,String>,Tuple2<String,String>>(){
			@Override
			public Iterable<Tuple2<String,String>> call(Tuple2<String,String> t)
			{
				ArrayList<Tuple2<String,String>> term_doc_commbinition_inner=new ArrayList<Tuple2<String,String>>();
				 String[] docs=t._2.split(",");
				 int length_of_doc_list=docs.length;
				 //if(length_of_doc_list>=threshold){//是否多余？
					 for(int d=0;d<length_of_doc_list;d++)
					 {
						 
						 
						 term_doc_commbinition_inner.add(new Tuple2<String,String>(docs[d],t._1));
						 
						 
						 /*for(int j=i+1;j<length_of_doc_list;j++)
						 {
							 
							 //term_doc_commbinition.add(new Tuple2<String,String>(docs[i]+" "+docs[j],t._1));
							 
							 //if()
							 
							 if(share(docs[i].toString(),docs[j].toString()))//只有在两个docid列表中有n-1个相同的元素时才进行合并
							 {
								 term_doc_commbinition.add(new Tuple2<String,String>(doc_combination(docs[i].toString(),docs[j].toString()).trim(),t._1));
							 }
						 }*/
					 }
			//	 }
			/*else{
					 term_doc_commbinition_inner.add(new Tuple2<String,String>(t._2,t._1));
				 }*/
				 return term_doc_commbinition_inner;
			}
		});//doc   itemset  doc1     安徽   合肥 
		
		
		JavaPairRDD<String,String> term_doc_list=term_doc_composition.mapToPair(new PairFunction<Tuple2<String,String>,String,String>(){
		//term_doc_list=term_doc_composition.mapToPair(new PairFunction<Tuple2<String,String>,String,String>(){
			@Override
			public Tuple2<String,String> call(Tuple2<String,String> t)
			{
				return new Tuple2<String,String>(t._1,t._2);
			}
		});
		//term_doc_list  key value doc  ,马鞍山
	    //doc  term
		
		
		JavaPairRDD<String,String> docs_terms=term_doc_list.reduceByKey(new Function2<String,String,String>(){
			@Override
			public String call(String s1,String s2)
			{
				/*HashMap<String,String> term_term=new HashMap<String,String>();
				
				
				
				if(term_term.containsKey(s2))
				{
					
				}*/
				
				String[]  str_list=s1.split("\\|");
				/*int length_of_itemset=str_list[0].split(" ").length;
				 if(length_of_itemset<iteration)
				{*/
					int str_list_length=str_list.length;
					int p=0;
					for(;p<str_list_length;p++)
					{
						if(str_list[p].equalsIgnoreCase(s2))
						{
							//return s1;
							break;
						}
					}
					if(p==str_list_length)
					{
						s1+="|"+s2;
					}
				/*}else if(length_of_itemset==iteration)
				{
					
				}*/
				return s1;
			}
		});//doci,安徽|马鞍山
		
		//docs_terms.saveAsTextFile("hdfs://master:8020/user/hdfs/lpp/data/test/frequent_mining/docs_terms/"+i);
		JavaRDD<Tuple2<String,String>> terms_docs_second=docs_terms.flatMap(new FlatMapFunction<Tuple2<String,String>,Tuple2<String,String>>(){
			@Override
			public Iterable<Tuple2<String,String>> call(Tuple2<String,String> t)
			{
				String[] second_terms=t._2.split("\\|");
				int tmp_length_of_itemset=second_terms[0].split(" ").length;
				ArrayList<Tuple2<String,String>> terms_docs_second_inner=new ArrayList<Tuple2<String,String>>();
				int second_terms_length=second_terms.length;
				HashSet<String> unique_terms=new HashSet<String>();
				ArrayList<Integer> unique_terms_list=new ArrayList<Integer>();
				
				
				if(tmp_length_of_itemset<iteration){
						for(int p=0;p<second_terms_length;p++)
						{
							String[] tmp_itemset=second_terms[p].split(" ");
							for(int s=0;s<tmp_length_of_itemset;s++)
							{
								if(unique_terms.add(tmp_itemset[s]))
								{
									unique_terms_list.add(Integer.parseInt(tmp_itemset[s]));
								}
							}
						}
						
						if(unique_terms.size()>iteration)
						{
							//if(second_terms_length>=2){
								for(int j=0;j<second_terms_length;j++)
								{
									for(int d=j+1;d<second_terms_length;d++)
									{
										if(share(second_terms[d],second_terms[j]))
										{
											//terms_docs_second_inner.add(new Tuple2<String,String>(second_terms[j],t._1));
											terms_docs_second_inner.add(new Tuple2<String,String>(term_combination(second_terms[j].toString(),second_terms[d].toString()).trim(),t._1));
										}
									}
								
								}
							//}
							/*else{
								terms_docs_second_inner.add(new Tuple2<String,String>(t._2,t._1));// if there is just one term, exchange position of t._1 with t._2
							}*/
						}else if(unique_terms.size()==iteration)
						{
							StringBuilder itemset=new StringBuilder("");
				    		Collections.sort(unique_terms_list);
				    		int length_of_unique_terms_list=unique_terms_list.size();
				    		for(int d=0;d<length_of_unique_terms_list;d++)
				    		{
				    			itemset.append(unique_terms_list.get(d)+" ");
				    		}
				    		terms_docs_second_inner.add(new Tuple2<String,String>(itemset.toString().trim(),t._1));
						}
				}else if(tmp_length_of_itemset==iteration)
				{
					for(int z=0;z<second_terms_length;z++)
					{
						terms_docs_second_inner.add(new Tuple2<String,String>(second_terms[z].toString(),t._1));
					}
				}
				return terms_docs_second_inner;
			}
		});//安徽  南京 ,doc
		 
		
		
		JavaPairRDD<String,String> terms_docs_second_pair=terms_docs_second.mapToPair(new PairFunction<Tuple2<String,String>,String,String>(){
			@Override
			public Tuple2<String,String> call(Tuple2<String,String> t)
			{
				return new Tuple2<String,String>(t._1,t._2);
			}
		});//安徽  南京 ,doc
		
		
		//terms_docs_second_pair.saveAsTextFile("hdfs://master:8020/user/hdfs/lpp/data/test/frequent_mining/tmp/"+i);
		
		
		
		terms_docs=terms_docs_second_pair.reduceByKey(new Function2<String,String,String>(){
			@Override
			public String call(String s1,String s2)
			{
				
				String[]  doc_list=s1.split(",");// in order to make sure
				int doc_list_length=doc_list.length;
				int p=0;
				for(;p<doc_list_length;p++)
				{
					if(doc_list[p].equalsIgnoreCase(s2))
					{
						//return s1;
						break;
					}
				}
				if(p==doc_list_length)
				{
					s1+=","+s2;
				}
				return s1;
				
				//return s1+","+s2;
			}
		});//中国   马鞍山, doci,docj
		
		
		
		//terms_docs.saveAsTextFile("hdfs://master:8020/user/hdfs/lpp/data/test/frequent_mining/tmp/"+i);
		
		if(i<iteration-1)
		{
			terms_docs_filtered=terms_docs.filter(new Function<Tuple2<String,String>,Boolean>()
					{
						@Override
				 		public Boolean call(Tuple2<String,String> t) {
							//i=1;
							if(t._1.split(" ").length<iteration){
								return t._2.split(",").length>=threshold;//this threshold is desired for frequent itemsets
							}else
							{
								return t._2.split(",").length>=1;
							}
							//return s.contains("MySQL"); 
				 			}
				
					});
		}else if(i==iteration-1)
		{
			terms_docs_filtered=terms_docs.filter(new Function<Tuple2<String,String>,Boolean>()
					{
						@Override
				 		public Boolean call(Tuple2<String,String> t) {
							//i=1;
							/*if(t._1.split(" ").length<iteration){
								return t._2.split(",").length>=threshold;//this threshold is desired for frequent itemsets
							}else
							{*/
								return t._2.split(",").length>=threshold;
							/*}*/
							//return s.contains("MySQL"); 
				 			}
				
					});
		}
		
		
		
		
		//terms_docs_filtered.saveAsTextFile("hdfs://master:8020/user/hdfs/lpp/Apriori_On_Item/"+i);
		/*JavaPairRDD<String,String>terms_docs_second=docs_terms。map(new Function<Tuple2<String,String>,Tuple2<String,String>>()
				{
				@Override 
				public Tuple2<String,String> call(Tuple2<String,String> s)
				{
					return new Tuple2<String,String>(s._1,s._2);
				}
			
				});*/
	    //terms_docs=terms_doc_frequent;//doc1 doc5,安徽
	    //terms_doc_frequent.saveAsTextFile("hdfs://master:8020/user/hdfs/lpp/data/test/frequent_mining/terms_doc_frequent/"+cur_counter);
		//terms_docs.saveAsTextFile("hdfs://master:8020/user/hdfs/lpp/data/test/frequent_mining/terms_doc_frequent/"+cur_counter);
	    //cur_counter=terms_docs.count();
		//terms_docs.saveAsTextFile("hdfs://master:8020/user/hdfs/lpp/data/test/frequent_mining/docs_terms/"+i);
	    
		}//for
		terms_docs_filtered.saveAsTextFile("hdfs://master:8020/user/hdfs/lpp/Apriori_On_Item/");
		//terms_docs.saveAsTextFile("hdfs://master:8020/user/hdfs/lpp/data/test/frequent_mining/terms_doc_frequent/");
		
		//term_doc_composition.saveAsTextFile("hdfs://master:8020/user/hdfs/lpp/data/test/frequent_mining");
		//terms_doc_frequent.saveAsTextFile("hdfs://master:8020/user/hdfs/lpp/data/test/frequent_mining");
		//terms_docs_filtered.saveAsTextFile("hdfs://master:8020/user/hdfs/lpp/Apriori_On_Item/");
		
	}
	
}
