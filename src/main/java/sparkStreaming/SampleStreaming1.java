package sparkStreaming;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class SampleStreaming1 {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("Sample Streaming").setMaster("local[*]");
		
		JavaStreamingContext  jsc = new JavaStreamingContext(conf, Durations.seconds(15));
		
		JavaDStream<String> lines = jsc.textFileStream("D:/Streaming/Data/Input");
		
		

		
		JavaDStream<String> newrdd = lines.repartition(3);
		newrdd.foreachRDD(new VoidFunction<JavaRDD<String>>(){

			public void call(JavaRDD<String> arg0) throws Exception {
				System.out.println("For an RDD "+arg0.getNumPartitions());
				arg0.foreach(new VoidFunction<String>(){

					public void call(String arg0) throws Exception {
						System.out.println(arg0);
						
					}
					
				});
				
			}
			
		});
		
	JavaDStream<String> tokens = 	newrdd.flatMap(new FlatMapFunction<String,String>(){

			public Iterator<String> call(String arg0) throws Exception {
				
				String[] listOfWords = arg0.split(" ");
				
				return Arrays.asList(listOfWords).iterator();
			}
			
		});
	
	
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
	
	i.print();
		
		jsc.start();
		try {
			jsc.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
	}

}
