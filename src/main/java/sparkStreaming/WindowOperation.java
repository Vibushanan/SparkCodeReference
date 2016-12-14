package sparkStreaming;

import java.util.Arrays;
import java.util.Iterator;

import javafx.util.Duration;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class WindowOperation {

	public static void main(String[] args) {
		
		
	SparkConf conf = new SparkConf().setAppName("Sample Streaming Window Operation").setMaster("local[*]");
		
		JavaStreamingContext  jsc = new JavaStreamingContext(conf, Durations.seconds(10));
		jsc.sparkContext().setCheckpointDir("D:/Streaming/Data/checkpoint");
		JavaDStream<String> lines = jsc.textFileStream("D:/Streaming/Data/Input");
		
		
		JavaDStream<String> tokens = 	lines.flatMap(new FlatMapFunction<String,String>(){

				public Iterator<String> call(String arg0) throws Exception {
					
					String[] listOfWords = arg0.split(" ");
					
					return Arrays.asList(listOfWords).iterator();
				}
				
			});
		
		
		JavaPairDStream<String, Integer> pairedStream = tokens.mapToPair(new PairFunction<String,String,Integer>(){

			public Tuple2<String, Integer> call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2(arg0,1);
			}
			
		});
		
		
		pairedStream.countByWindow(Durations.seconds(30),Durations.seconds(20)).print();
		
		

		jsc.start();

		try {
			jsc.awaitTermination();
		} catch (InterruptedException e) {

			e.printStackTrace();
		}

	}

}
