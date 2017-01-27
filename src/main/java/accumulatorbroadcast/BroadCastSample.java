package accumulatorbroadcast;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

public class BroadCastSample {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("Sample").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		
		Broadcast<int[]> i =sc.broadcast(new int[] {1,2,3,4,5});
		
		
		
		System.out.println(i.getValue().length);
		
		
		
	}

}
