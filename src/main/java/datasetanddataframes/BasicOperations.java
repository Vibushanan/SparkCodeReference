package datasetanddataframes;

import java.util.Collections;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
public class BasicOperations {

	public static void main(String[] args) {
		
		
		SparkSession spark = SparkSession
				  .builder()
				  .appName("Java Spark SQL basic example")
				  .master("local[*]")
				  .config("spark.sql.warehouse.dir", "file:///D:/Downloads/UIDAI-ENR-DETAIL-20160907-20160913/") 
				  .getOrCreate();
		
		
		
	Dataset<Row> df=	spark.read().csv("D:/Downloads/UIDAI-ENR-DETAIL-20160907-20160913/UIDAI-ENR-DETAIL-20160907.csv");
		
	

df.select("*").where(col("_c7").$eq$eq$eq(10));
/*this can also be represented as 
 * 			df.select("*").where(df.col("_c7").$eq$eq$eq(10)).show();
 * former is preferable
 */


/*df.createOrReplaceTempView("Aadhar");

spark.sql("select _c1 from global_temp.Aadhar").show();

*/




/*
 *  Dataset<Row> ---> Dataframes
 *  Dataset<Class> ---> Dataset
 *  
 *  DataFrames/Datasets Can be created from many sources and 
 * 	
 * 	Dataframe to Dataset and vise versa
 */
	Dataset<String> dataset = df.as(Encoders.STRING());

	Dataset<Row> df1= dataset.toDF();
	JavaRDD<String> rdd  = df.toJavaRDD().map(r -> {
		
		
		return r.mkString();
		
	});
/* 
 *  RDD to Others
 * 
 * RDD to Dataframe
 */


Dataset<Row> rrr = spark.createDataFrame(rdd, String.class);





//RDD to dataset

Dataset<String> rrr2 = spark.createDataset(rdd.toRDD(rdd), Encoders.STRING());


	}

}
