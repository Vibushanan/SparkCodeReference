package sparkStreaming;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.google.common.io.Files;

import scala.Tuple2;

public class SparkStreamingStateFul {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("Sample Streaming")
				.setMaster("local[*]");
		
		JavaStreamingContext jsc = new JavaStreamingContext(conf,
				Durations.seconds(15));
		
		//Download and put his against HADOOP_HOME
		//https://codeload.github.com/srccodes/hadoop-common-2.2.0-bin/zip/master
		
		
		jsc.sparkContext().setCheckpointDir("D:/Streaming/Data/checkpoint");
	
// Make Sure this points to a directory
		JavaDStream<String> lines = jsc
				.textFileStream("D:/Streaming/Data/Input");

	
		JavaDStream<String> tokens = lines
				.flatMap(new FlatMapFunction<String, String>() {

					public Iterator<String> call(String arg0) throws Exception {

						String[] listOfWords = arg0.split(" ");

						return Arrays.asList(listOfWords).iterator();
					}

				});
		
		
		JavaPairDStream<String, Integer> token_Pair = tokens.mapToPair(new PairFunction<String,String,Integer>(){

			public Tuple2<String, Integer> call(String arg0) throws Exception {
				
				return new Tuple2(arg0,1);
			}
			
		});
		
		token_Pair.print();
		
		
		
		
		token_Pair.updateStateByKey(new Function2<List<Integer>,Optional<Integer>,Optional<Integer>>(){

			public Optional<Integer> call(List<Integer> arg0,
					Optional<Integer> arg1) throws Exception {
				int sum = arg1.or(0);
				for(int i : arg0){
					sum+=i;
				}
				
				return Optional.of(sum);
			}
			
			
			
		}).print();
		
		jsc.start();

		try {
			jsc.awaitTermination();
		} catch (InterruptedException e) {

			e.printStackTrace();
		}

	}
}
