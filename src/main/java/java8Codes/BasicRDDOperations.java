package java8Codes;

import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

public class BasicRDDOperations {
	
	
	public static void main(String args[]){
		SparkConf conf = new SparkConf().setAppName("Sample").setMaster("local[*]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> distFile = sc.textFile("D:/Downloads/UIDAI-ENR-DETAIL-20160907-20160913/UIDAI-ENR-DETAIL-20160911.csv");
		
		
		JavaRDD<String> newRDD=	distFile.map(mp -> {
			String[] rec = mp.split(",");
			
			
			return rec[0]+","+rec[2];
			
			
		});
		
		
		JavaPairRDD<String,Integer> pairRDD=	distFile.mapToPair(pp -> {
			
			String[] rec = pp.split(",");
			
			return new Tuple2<String,Integer>(rec[0],Integer.parseInt(rec[2]));
					
		}).sortByKey();
		
		Map<String, Long> map= pairRDD.countByKey();
		
						
		JavaPairRDD<String,Integer> rbkey = pairRDD.reduceByKey((x,y) -> x+y);
		
		
		
		
		System.out.println(map);
		
		//System.out.println(distFile.collect());
		
		
	}

}
