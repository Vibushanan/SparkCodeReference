package datasetanddataframes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
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
		//
	
	
	
/*
df.select("*").where(col("_c7").$eq$eq$eq(10));
this can also be represented as 
 * 			df.select("*").where(df.col("_c7").$eq$eq$eq(10)).show();
 * former is preferable
 


df.createOrReplaceTempView("Aadhar");

spark.sql("select _c1 from global_temp.Aadhar").show();







 *  Dataset<Row> ---> Dataframes
 *  Dataset<Class> ---> Dataset
 *  
 *  DataFrames/Datasets Can be created from many sources and 
 * 	
 * 	Dataframe to Dataset and vise versa
 
	Dataset<String> dataset = df.as(Encoders.STRING());

	Dataset<Row> df1= dataset.toDF();
	JavaRDD<String> rdd  = df.toJavaRDD().map(r -> {
		
		
		return r.mkString();
		
	});
 
 *  RDD to Others
 * 
 * RDD to Dataframe
 


Dataset<Row> rrr = spark.createDataFrame(rdd, String.class);



*/

//RDD to dataset

//Dataset<String> rrr2 = spark.createDataset(rdd.toRDD(rdd), Encoders.STRING());


df.show();


JavaRDD<Row> data =  df.toJavaRDD();

JavaRDD<String> data2 = data.mapPartitionsWithIndex(new Function2<Integer,Iterator<Row>,Iterator<String>>(){

	@Override
	public Iterator<String> call(Integer v1, Iterator<Row> v2) throws Exception {
		// TODO Auto-generated method stub
		
		List<String> ret= new ArrayList<String>();
		System.out.println("Index   :"+v1);
		if(v1 == 0){
			
			if(v2.hasNext()){
				System.out.println(v2.next().toString());
				while(v2.hasNext()){
					ret.add(v2.next().toString());
				}
			}
			
		}
		
		return ret.iterator();
	}
	
}, true);

Dataset<String> ds = spark.createDataset(data2.rdd(), Encoders.STRING());




	}
	
	
	

}
