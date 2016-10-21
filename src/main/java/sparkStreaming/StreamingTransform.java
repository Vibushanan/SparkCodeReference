package sparkStreaming;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
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

		/*
		 * When more than one Spark Context needs to be running on the same VM 
		 * 
		 * 1. Set spark.driver.allowMultipleContexts to true
		 * 
		 * 2. With That Create a Spark Context
		 * 
		 * 3. Using the Spark Context create the 2 Spark Contexts as below 
		 * 
		 */
		
		
		
		/*
		 *  Below Steps Illustrate The Xle Context Creation
		 * 
		 ************************************************************************************************/
		
		SparkConf conf = new SparkConf().setAppName("Transform Streaming")
				.setMaster("local[*]").set("spark.driver.allowMultipleContexts", "true");
		
		
		
		SparkContext sc = new SparkContext(conf);
		
		
		
		final JavaSparkContext jsctxt = new JavaSparkContext(sc);
		
		
		JavaStreamingContext jsc = new JavaStreamingContext(JavaSparkContext.fromSparkContext(sc),
				Durations.seconds(15));
		
		
		//***********************************************************************************************
		
		
		

		JavaDStream<String> lines = jsc
				.textFileStream("D:/Streaming/Data/Input/");

		
	
		 final JavaPairRDD<String, String> RDD3 = jsctxt.parallelizePairs(
		         Arrays.asList(
		           new Tuple2<String, String>("Vibushanan", "One"),
		           new Tuple2<String, String>("Shyamala","Two"),
		           new Tuple2<String, String>("Somasundaram","Three"),
		           new Tuple2<String, String>("Saahil","Four")));
		
		
		JavaDStream<String> tokens = 	lines.flatMap(new FlatMapFunction<String,String>(){

			public Iterator<String> call(String arg0) throws Exception {
				
				String[] listOfWords = arg0.split(" ");
				
				return Arrays.asList(listOfWords).iterator();
			}
			
		});
	
		
		
	JavaPairDStream<String, Integer> tokenPairs = tokens.mapToPair(new PairFunction<String,String,Integer>(){

		public Tuple2<String, Integer> call(String arg0) throws Exception {
			// TODO Auto-generated method stub
			return new Tuple2(arg0,1);
		}
		
	});
	
	
	tokenPairs.print();
		RDD3.cache();
		
		
		RDD3.foreach(new VoidFunction<Tuple2<String,String>>(){

			public void call(Tuple2<String, String> t)
					throws Exception {
				System.out.println(t._1+"  and  "+t._2);
				
			}
  			
  		});
	tokenPairs.transformToPair(new Function<JavaPairRDD<String,Integer>,JavaPairRDD<String, Tuple2<Integer, String>>>(){
		
		
	
		public JavaPairRDD<String, Tuple2<Integer, String>> call(JavaPairRDD<String, Integer> v1)
				throws Exception {
			// TODO Auto-generated method stub
			
			if (!v1.isEmpty()){
				  System.out.println("Got  a Stream of Data "+v1.count()+" : ");
				  		if(RDD3 != null){
				  		System.out.println("There Man");	
				  		
				  		RDD3.foreach(new VoidFunction<Tuple2<String,String>>(){

							public void call(Tuple2<String, String> t)
									throws Exception {
								System.out.println(t._1+"  and  "+t._2);
								
							}
				  			
				  		});
				  			
				  		}
				}else{
					System.out.println("No Stream is there ");
					
				}
			return v1.join(RDD3);
		}
		
	}).print();
	

	
	jsc.start();
	try {
		jsc.awaitTermination();
	} catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}

	}

}
