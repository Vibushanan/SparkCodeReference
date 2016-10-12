package sparkStreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkStreamingCustomRec {

	public static void main(String[] args) {


		SparkConf conf = new SparkConf().setAppName("Sample Streaming").setMaster("local[*]");
		
		JavaStreamingContext  jsc = new JavaStreamingContext(conf, Durations.seconds(15));
		
		JavaDStream<String> lines=jsc.receiverStream(new CustomReceiver("D:/Streaming/"));
									
		lines.print();
		
		jsc.start();
		try {
			jsc.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
									

	}

}
