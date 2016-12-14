package com.slk.spark.SparkCodeBase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import com.slk.core.RDDOperations.NamePartitioner;

import scala.Tuple2;

public class Practise {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
SparkConf conf = new SparkConf().setAppName("Sample").setMaster("local[*]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> distFile = sc.textFile("D:/Downloads/UIDAI-ENR-DETAIL-20160907-20160913/UIDAI-ENR-DETAIL-20160907.csv");
		
		
	
		         List<String> st = new ArrayList<String>();
		         
		         st.add("Vibu");
		         st.add("Shyam");
		         st.add("Vibu");
		         
		        JavaRDD<String> i =  sc.parallelize(st);
		        
		        
		        System.out.println(i.partitioner());
		        System.out.println(i.partitions());
		        
		        System.out.println("Num Partitions   : "+i.getNumPartitions());
		        
		        
		        
		        
		        System.out.println("Num Partitions 1  : "+i.coalesce(6).getNumPartitions());
		        
		        
		   JavaRDD<Tuple2<String, Integer>> j =   i.map(new Function<String,Tuple2<String,Integer>>(){

				public Tuple2<String, Integer> call(String v1) throws Exception {
					// TODO Auto-generated method stub
					return new Tuple2(v1,1);
				}
		    	 
		     });
		        
		  		   
		   
		         
		
		JavaRDD<String> ststs = distFile.mapPartitionsWithIndex(new Function2<Integer,Iterator<String>,Iterator<String>>(){

			public Iterator<String> call(Integer v1, Iterator<String> v2)
					throws Exception {
				
				System.out.println("Index   :"+v1);
				ArrayList<String> states = new ArrayList<String>();
				//if(v1 != 0){
					int cnt =0;
					while(v2.hasNext()){
						
						String[] split = v2.next().split(",");
						//System.out.println("Index   :"+v1 +"Count    :   "+cnt+++"    Data   :   " +split[0]);
						states.add(split[0]);
					}
					
				//}
				return states.iterator();
			}
			
		}, false);
		
		
		
		JavaPairRDD<String, Integer> spair  = ststs.mapToPair(new PairFunction<String,String,Integer>(){

			public Tuple2<String, Integer> call(String t) throws Exception {
				// TODO Auto-generated method stub
				
			
				
				return new Tuple2(t,1);
			}
			
		});
		
		
		System.out.println("  =========>   "+spair.getNumPartitions());
		
		
		JavaPairRDD<String, Integer> splt = spair.partitionBy(new NamePartitioner(3));
		
		System.out.println("  =========>   "+splt.getNumPartitions());
		
		
	
		spair.groupByKey().map(new Function<Tuple2<String,Iterable<Integer>>,Tuple2<String,Integer>>(){

			public Tuple2<String, Integer> call(
					Tuple2<String, Iterable<Integer>> v1) throws Exception {
				// TODO Auto-generated method stub
				return null;
			}
			
		});
		
		
		
		spair.foreach(new VoidFunction<Tuple2<String,Integer>>(){

			public void call(Tuple2<String, Integer> t) throws Exception {
				System.out.println(t._1()+"  "+t._2());
				
			}
			
		});
		
		/*ststs.foreach(new VoidFunction<String>(){

			public void call(String t) throws Exception {
				
				System.out.println(t);
			}
			
		});*/
		
		System.out.println("parellism  :"+sc.defaultParallelism());
	}

}
