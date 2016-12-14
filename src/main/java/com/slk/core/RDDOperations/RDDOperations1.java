package com.slk.core.RDDOperations;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

public class RDDOperations1 {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("Sample").setMaster("local[*]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> distFile = sc.textFile("D:/Downloads/UIDAI-ENR-DETAIL-20160907-20160913/UIDAI-ENR-DETAIL-20160911.csv");
		
		System.out.println("Partitions :"+distFile);
		
		
		System.out.println("parellism  :"+sc.defaultParallelism());
		
		//        Prints an  RDD
		/*distFile.foreach(new VoidFunction<String>(){

			public void call(String arg0) throws Exception {
				
				
			System.out.println(arg0);	
				
			}	
			
		});*/
		
		// run a map transformation to select only Registrar,Enrolment Agency,State,District,Gender,Age,Residents providing email (0 to 3 ,6,7,10)

		
		
		JavaRDD<String> mapRDD =distFile.map(new Function<String,String>(){

			public String call(String line) throws Exception {
			
				String[] str = line.split(",");
				
				return  str[0]+","+str[1]+","+str[2]+","+str[3]+","+str[6]+","+str[7]+","+str[10];
			}
			
		});
		
		
		JavaRDD<String> mapRDD1 =distFile.flatMap(new FlatMapFunction<String,String>(){

			public Iterator<String> call(String line) throws Exception {
			
				String[] str = line.split(",");
				
				return  Collections.singletonList(str[0]+","+str[1]+","+str[2]+","+str[3]+","+str[6]+","+str[7]+","+str[10]).iterator();
			}
			
		});
		
				
		
		//.sample(false, 0.2, 0);
		
		
		/*System.out.println(mapRDD.getNumPartitions());
		
		System.out.println(mapRDD.getStorageLevel());
		
		System.out.println(mapRDD.getClass());*/
		
		
		
	JavaPairRDD<String, String> pairRDD = mapRDD.mapToPair(new PairFunction<String,String,String>(){

		public Tuple2<String, String> call(String line) throws Exception {
			
			String[] str = line.split(",");
			
			
			return new Tuple2(str[2],str[4]+","+str[5]);
		}
		
	});
	
	
 JavaPairRDD<String, String> fnRDD = pairRDD.filter(new Function<Tuple2<String,String>,Boolean>(){

		public Boolean call(Tuple2<String, String> arg0) throws Exception {
			// TODO Auto-generated method stub
			
			if(arg0._1.equalsIgnoreCase("Tamil Nadu")){
				return true;
			}
			
			return false;
		}
		
	});
 
 
 JavaPairRDD<String, Integer> ggg = fnRDD.mapToPair(new PairFunction<Tuple2<String,String>,String,Integer>(){

	public Tuple2<String, Integer> call(Tuple2<String, String> arg0)
			throws Exception {
		// TODO Auto-generated method stub
		return new Tuple2(arg0._2().split(",")[0],Integer.parseInt(arg0._2().split(",")[1]));
	}
	 
	 
	 
 });
 
 System.out.println(ggg.count());
 
 ggg.aggregateByKey(new HashSet<Integer>(), new Function2<Set<Integer>, Integer, Set<Integer>>(){

	public Set<Integer> call(Set<Integer> arg0, Integer arg1) throws Exception {
		// TODO Auto-generated method stub
		 arg0.add(arg1);
		 
		 return arg0;
	}
	 
 }
, new Function2<Set<Integer>, Set<Integer>, Set<Integer>>(){

	public Set<Integer> call(Set<Integer> arg0, Set<Integer> arg1)
			throws Exception {
		// TODO Auto-generated method stub
		
		arg0.addAll(arg1);
		return arg0;
	}
	
});
 
 ggg.foreach(new VoidFunction<Tuple2<String,Integer>>(){

	public void call(Tuple2<String, Integer> arg0) throws Exception {
		// TODO Auto-generated method stub
		System.out.println(arg0._1()+"   ::::::   "+arg0._2);
	}
	 
 });
 

 /*.reduceByKey(new Function2<Integer,Integer,Integer>(){

	public Integer call(Integer arg0, Integer arg1) throws Exception {
		// TODO Auto-generated method stub
		return arg0+arg1;
	}
	 
 }).foreach(new VoidFunction<Tuple2<String,Integer>>(){

	public void call(Tuple2<String, Integer> arg0) throws Exception {
		System.out.println(arg0._1()+"   "+arg0._2());
		
	}
	 
	 
 });;
*/
 
 
	
/*	
	.mapToPair(new PairFunction<Tuple2<String,String>,String,Integer>(){

		public Tuple2<String, Integer> call(Tuple2<String, String> line)
				throws Exception {
			// TODO Auto-generated method stub
			return null;
		}
		
	})*/
	
/*	mapToPair(new PairFunction<Tuple2<String,String>,String,Integer>(){

		public Tuple2<String, Integer> call(
				Tuple2<String, String> arg0) throws Exception {
		System.out.println(arg0._1()+"  and "+arg0._2());
			
			String[] st = arg0._2().split(",");
			
			return new Tuple2(arg0._1,Integer.parseInt(st[1])) ;
		}
		
	});*/
		
 
 
 fnRDD.collect();
 
	//System.out.println(fnRDD.re);
	
	
	//pairRDD.groupByKey().cache();
	
	//pairRDD.groupByKey().persist(StorageLevel.MEMORY_ONLY());
	
	
	
	
	
	//Things u can see about an RDD
	/*System.out.println(distFile.count()); //169954
	System.out.println("Correct Count "+distFile.distinct().count()); //169954
	System.out.println("0.5 : "+distFile.countApproxDistinct(0.5));   //170431
	System.out.println("0.05 : "+distFile.countApproxDistinct(0.05));  //161714
	System.out.println("0.005 : "+distFile.countApproxDistinct(0.005)); //169403
*/	
	
	/*try {
		System.out.println(distFile.collectAsync().get().size());
	} catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (ExecutionException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
	
	
	
	System.out.println(distFile.first());
	
	
	System.out.println(pairRDD.id());
	
	
	System.out.println(pairRDD.partitioner().isPresent());*/
	
	
	}

}
