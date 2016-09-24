package com.slk.core.RDDOperations;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

public class AggregateFunction {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf conf = new SparkConf().setAppName("Sample").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		
		JavaPairRDD<Integer, Integer> pairs = sc.parallelizePairs(
				         Arrays.asList(
				           new Tuple2<Integer, Integer>(1, 1),
				           new Tuple2<Integer, Integer>(3, 2),
				           new Tuple2<Integer, Integer>(5, 1),
				           new Tuple2<Integer, Integer>(5, 3),
				           new Tuple2<Integer, Integer>(5, 4)), 2);
		
		Set<Integer> initial = new HashSet<Integer>();
		initial.add(0);
	
		JavaPairRDD<Integer, Set<Integer>> sets = pairs.aggregateByKey(initial,
				         new Function2<Set<Integer>, Integer, Set<Integer>>() {
				           public Set<Integer> call(Set<Integer> a, Integer b) {
				             a.add(b);
				             return a;
				           }
				         },
				         new Function2<Set<Integer>, Set<Integer>, Set<Integer>>() {
				           public Set<Integer> call(Set<Integer> a, Set<Integer> b) {
				             a.addAll(b);
				             return a;
				           }
				         });
		
		
		System.out.println(sets.collectAsMap());
		

		JavaPairRDD<Integer, Integer> sets1  = pairs.aggregateByKey(new Integer(0), new Function2<Integer,Integer,Integer>(){

			public Integer call(Integer arg0, Integer arg1) throws Exception {
				// TODO Auto-generated method stub
				return arg0+arg1;
			}
			
		}, new Function2<Integer,Integer,Integer>(){

			public Integer call(Integer arg0, Integer arg1) throws Exception {
				// TODO Auto-generated method stub
				return arg0+arg1;
			}
			
		});

		System.out.println(sets1.collectAsMap());
		
		
			}

}
