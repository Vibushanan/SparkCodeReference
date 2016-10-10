package sparkStreaming;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class StreamingTransform {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("Transform Streaming")
				.setMaster("local[*]").set("spark.driver.allowMultipleContexts", "true");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaStreamingContext jsc = new JavaStreamingContext(conf,
				Durations.seconds(15));

		JavaDStream<String> lines = jsc
				.textFileStream("D:/Streaming/Data/Input/");

		
		final JavaPairRDD<String, String> RDD1 = sc.parallelizePairs(
		         Arrays.asList(
		           new Tuple2<String, String>("Vibushanan", "Tuticorin"),
		           new Tuple2<String, String>("Shyamala","Vellore"),
		           new Tuple2<String, String>("Somasundaram","Nagercoil"),
		           new Tuple2<String, String>("Saahil","Bengaluru")), 2);
		
		
		
		JavaDStream<String> tokens = 	lines.flatMap(new FlatMapFunction<String,String>(){

			public Iterator<String> call(String arg0) throws Exception {
				
				String[] listOfWords = arg0.split(" ");
				
				return Arrays.asList(listOfWords).iterator();
			}
			
		});
	
		tokens.print();
	JavaPairDStream<String, Integer> i = tokens.mapToPair(new PairFunction<String,String,Integer>(){

		public Tuple2<String, Integer> call(String arg0) throws Exception {
			// TODO Auto-generated method stub
			return new Tuple2(arg0,1);
		}
		
	}).reduceByKey(new Function2<Integer,Integer,Integer>(){

		public Integer call(Integer arg0, Integer arg1) throws Exception {
			// TODO Auto-generated method stub
			return arg0+arg1;
		}
		
	});
	
	
	i.transformToPair(new Function<JavaPairRDD<String,Integer>,JavaPairRDD<String, Integer> >(){

		public JavaPairRDD<String, Integer> call(
				JavaPairRDD<String, Integer> staticrdd) throws Exception {
			
			return staticrdd.sortByKey();
		}
		
	}).print();
	
	
	//i.print();
	
	jsc.start();
	try {
		jsc.awaitTermination();
	} catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}

	}

}
