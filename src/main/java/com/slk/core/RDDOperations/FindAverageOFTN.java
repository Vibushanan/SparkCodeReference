package com.slk.core.RDDOperations;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;

import scala.Tuple2;

public class FindAverageOFTN {

	public static void main(String[] args) {
			SparkConf conf = new SparkConf().setAppName("Sample").setMaster("local[*]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		Broadcast<int[]> bcastvar = sc.broadcast(new int[]{1,2,3});
		
		JavaPairRDD<String, Integer> RDD1 = sc.parallelizePairs(
		         Arrays.asList(
		           new Tuple2<String, Integer>("Vibushanan", 2000),
		           new Tuple2<String, Integer>("Shyamala",3000),
		           new Tuple2<String, Integer>("Vibushanan",4000),
		           new Tuple2<String, Integer>("Vibushanan",70000),
		           new Tuple2<String, Integer>("Shyamala",700000)), 2);
		
		JavaPairRDD<String, String> RDD2 = sc.parallelizePairs(
		         Arrays.asList(
		           new Tuple2<String, String>("Vibushanan", "Father"),
		           new Tuple2<String, String>("Shyamala","Mother"),
		           new Tuple2<String, String>("Saahil","Son"),
		           new Tuple2<String, String>("Shenbagam","Granny"),
		           new Tuple2<String, String>("Somasundaram","GrandPA")), 2);
		
		bcastvar.value();
		LongAccumulator accum = sc.sc().longAccumulator();
		accum.add(0l);
		//Join
		RDD1.join(RDD2).foreach(new VoidFunction<Tuple2<String,Tuple2<Integer,String>>>(){

		public void call(Tuple2<String, Tuple2<Integer, String>> arg0)
				throws Exception {

					System.out.println(arg0._1()+"   :   "+arg0._2._1+" - "+arg0._2()._2);
			
		}
		
	});
		
		System.out.println(RDD1.toDebugString());
		
		
		
	/*	
		System.out.println("--------------Join-----------------");
		RDD1.cogroup(RDD2).foreach(new VoidFunction<Tuple2<String,Tuple2<Iterable<Integer>,Iterable<String>>>>(){

			public void call(
					Tuple2<String, Tuple2<Iterable<Integer>, Iterable<String>>> arg0)
					throws Exception {
				// TODO Auto-generated method stub
			
				System.out.println();
				System.out.println("Name  : "+arg0._1()+"   :  ");
				
				
				Iterator itr1 = arg0._2._1().iterator();
				Iterator itr2 = arg0._2._2().iterator();
				System.out.println("--------------int1--------------");
				while(itr1.hasNext()){
					System.out.println(itr1.next()+"   ");
				}
				System.out.println("--------------int2--------------");
				while(itr2.hasNext()){
					System.out.println("  "+itr2.next());
				}
				System.out.println("--------------int3--------------");
			}
			
		});
		System.out.println("--------------Join-----------------");
		
	*/
		
		JavaRDD<String> distFile = sc.textFile("D:/Downloads/UIDAI-ENR-DETAIL-20160907-20160913/UIDAI-ENR-DETAIL-20160911.csv");
		
		JavaPairRDD<String, Integer> pairRDD = distFile.mapToPair(new PairFunction<String,String,Integer>(){

			public Tuple2<String, Integer> call(String arg0) throws Exception {
				String[] line = arg0.split(",");
				
				
				
				return  new Tuple2(line[2]+","+line[6],Integer.parseInt(line[7]));
			}
			
		});
		
		
		pairRDD.cache();
		
		RDD1.combineByKey(
				
		new Function<Integer,Tuple2<Integer,Integer>>(){

			public Tuple2<Integer,Integer> call(Integer arg0) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2(arg0,1);
			}
			
		}, new Function2<Tuple2<Integer,Integer>,Integer,Tuple2<Integer,Integer>>(){

			public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> arg0,
					Integer arg1) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2(arg0._1()+arg1,arg0._2+1);
			}
			
		}, new Function2<Tuple2<Integer,Integer>,Tuple2<Integer,Integer>,Tuple2<Integer,Integer>>(){

			public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> arg0,
					Tuple2<Integer, Integer> arg1) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2(arg0._1()+arg1._1(),arg0._2()+arg1._2());
			}
			
		});
		
		
		/*.foreach(new VoidFunction<Tuple2<String,Tuple2<Integer,Integer>>>(){

			public void call(Tuple2<String, Tuple2<Integer, Integer>> arg0)
					throws Exception {
				System.out.println(arg0._1()+"   :  "+arg0._2()._1()+" -- "+arg0._2()._2());
				
			}
			
		});;
*/		//createCombiner
		//mergeValue
//		/mergeCombiner
		
		
	/*	pairRDD.combineByKey(new Function<Integer,Integer>(){

			public Integer call(Integer arg0) throws Exception {
				// TODO Auto-generated method stub
				return 1;
			}
			
		}, new Function2<Integer,Integer,Integer>(){

			public Integer call(Integer arg0, Integer arg1) throws Exception {
				// TODO Auto-generated method stub
				return arg0+arg0;
			}
			
		}, new Function2<Integer,Integer,Integer>(){

			public Integer call(Integer arg0, Integer arg1) throws Exception {
				// TODO Auto-generated method stub
				return arg1/arg0;
			}
			
		}).foreach(new VoidFunction<Tuple2<String,Integer>>(){

			public void call(Tuple2<String, Integer> arg0) throws Exception {
				// TODO Auto-generated method stub
				System.out.println(arg0._1 + "   "+arg0._2);
			}
			
		});;*/
		
		
		JavaPairRDD<String, Integer> total = pairRDD.reduceByKey(new Function2<Integer,Integer,Integer>(){

			public Integer call(Integer arg0, Integer arg1) throws Exception {
				// TODO Auto-generated method stub
				return arg0+arg1;
			}
			
		});
		
		
		
		final Map<String, Long> count  = pairRDD.countByKey();
		
		
		JavaRDD<Tuple2<String, Integer>> resRDD = total.map(new Function<Tuple2<String,Integer>,Tuple2<String,Integer>>(){

			public Tuple2<String, Integer> call(Tuple2<String, Integer> arg0)
					throws Exception {
				// TODO Auto-generated method stub
				
				return new Tuple2(arg0._1,Math.round(arg0._2.doubleValue()/count.get(arg0._1)));
			}
			
		});
		
		

		JavaRDD<Tuple2<String, Integer>> resSortedRDD  =	resRDD.sortBy(new Function<Tuple2<String,Integer>,Double>(){

			public Double call(Tuple2<String, Integer> arg0) throws Exception {
				// TODO Auto-generated method stub
				Double d1 = new Double(arg0._2().longValue());
				
				return new Double(arg0._2.doubleValue());
			}
			
		}, true, 1);
		
		/*resSortedRDD.foreach(new VoidFunction<Tuple2<String,Integer>>(){

			public void call(Tuple2<String, Integer> arg0) throws Exception {
				// TODO Auto-generated method stub
				System.out.println(arg0._1()+"   :   "+arg0._2());
			}
			
		});
		*/
}
}