package accumulatorbroadcast;

import java.util.Arrays;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class AccumulatorSample {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Sample").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		
		JavaPairRDD<Integer, Integer> pairs = sc.parallelizePairs(
				         Arrays.asList(
				           new Tuple2<Integer, Integer>(1, 1),
				           new Tuple2<Integer, Integer>(3, 2),
				           new Tuple2<Integer, Integer>(5, 1),
				           new Tuple2<Integer, Integer>(5, 3),
				           new Tuple2<Integer, Integer>(5, 4)), 2);
		
		
		final Accumulator<Integer> accum = sc.accumulator(0);
		
		
		pairs.foreach(new VoidFunction<Tuple2<Integer,Integer>>(){

			public void call(Tuple2<Integer, Integer> arg0) throws Exception {
				
				accum.add(arg0._1());
			}
			
		});
		
		System.out.println(accum.value());

		
		System.out.println(pairs.count());

	}

}
