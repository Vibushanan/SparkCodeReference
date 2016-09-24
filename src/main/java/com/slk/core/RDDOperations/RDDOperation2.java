package com.slk.core.RDDOperations;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class RDDOperation2 {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Sample").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		
		JavaPairRDD<String, String> RDD1 = sc.parallelizePairs(
				         Arrays.asList(
				           new Tuple2<String, String>("Vibushanan", "car"),
				           new Tuple2<String, String>("Shyamala","House"),
				           new Tuple2<String, String>("Vibushanan","Bike"),
				           new Tuple2<String, String>("Vibushanan","TV"),
				           new Tuple2<String, String>("Shyamala","Cot")), 2);
		
		
		
		
		JavaPairRDD<String, String> RDD2 = sc.parallelizePairs(
		         Arrays.asList(
		           new Tuple2<String, String>("Vibushanan", "father"),
		           new Tuple2<String, String>("Shyamala","mommy"),
		           new Tuple2<String, String>("Saahil","son"))).partitionBy(new NamePartitioner(3));
		
		System.out.println("Before  :"+RDD2.getNumPartitions());
		
	//RDD2.repartitionAndSortWithinPartitions(new NamePartitioner());
	
	
 RDD2.partitionBy(new NamePartitioner(2));
	
	System.out.println("After   :"+RDD2.getNumPartitions());
		
		JavaPairRDD<String, Iterable<String>> RDD3 = RDD1.groupByKey();
		
	//System.out.println(RDD2.leftOuterJoin(RDD3).collectAsMap());
		

	}

}
