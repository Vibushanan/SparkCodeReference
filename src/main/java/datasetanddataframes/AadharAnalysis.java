package datasetanddataframes;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
public class AadharAnalysis {

	public static void main(String[] args) {

		SparkSession spark = SparkSession
				  .builder()
				  .appName("Java Spark SQL basic example")
				  .master("local[*]")
				  .config("spark.sql.warehouse.dir", "file:///D:/Downloads/UIDAI-ENR-DETAIL-20160907-20160913/") 
				  .getOrCreate();
		
		
	Encoder<Aadhar>	 aadharEncoder = Encoders.bean(Aadhar.class);
	Dataset<Aadhar> dataset=	spark.read().option("header", "true").csv("D:/Downloads/UIDAI-ENR-DETAIL-20160907-20160913/edited.csv").as(aadharEncoder);
	dataset.show();
	
	System.out.println(dataset.filter(dataset.col("header").equalTo("Allahabad Bank")).count());
	
	
	dataset.groupBy("State", "District").count().orderBy(col("count").desc()).show();
	
person p1= new person();
person p2= new person();
person p3= new person();
p1.setAge(30);
p1.setName("Vibu");
p3.setAge(30);
p3.setName("Vibu");
p2.setAge(1);
p2.setName("Saahi");
Encoder<person>	 personEncoder = Encoders.bean(person.class);
List<person> l1=new ArrayList<person>();
l1.add(p1);
l1.add(p2);
l1.add(p3);
	Dataset<person> person = spark.createDataset(l1,personEncoder );
	
	person.show();
	
	person.agg(org.apache.spark.sql.functions.sum(col("age"))).show();

	person.cube(col("age"),col("name")).count().show();
	person.rollup(col("age"),col("name")).count().show();
	}

	
	public static class Aadhar {

		private String header;
		public String getHeader() {
			return header;
		}

		public void setHeader(String header) {
			this.header = header;
		}

		private String Enrolment_Agency;
		private String State;
		private String District;
		
		

		

		public String getEnrolment_Agency() {
			return Enrolment_Agency;
		}

		public void setEnrolment_Agency(String enrolment_Agency) {
			Enrolment_Agency = enrolment_Agency;
		}

		public String getState() {
			return State;
		}

		public void setState(String state) {
			State = state;
		}

		public String getDistrict() {
			return District;
		}

		public void setDistrict(String district) {
			District = district;
		}
	}
		public static class person {

			private String Name;
			public String getName() {
				return Name;
			}
			public void setName(String name) {
				Name = name;
			}
			public int getAge() {
				return age;
			}
			public void setAge(int age) {
				this.age = age;
			}
			private int age;
			
			
			
	}
}
