
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

val conf = {
   new SparkConf()
     .setAppName("Spark-HDFS-Read-Write")
 }

 val sqlContext = new SQLContext(sc)

 val sc = new SparkContext(conf)

 val hdfs = "hdfs:///"
 val df = Seq((1, "Name1")).toDF("id", "name")

 //  Writing file in CSV format
 df.write.format("com.databricks.spark.csv").mode("overwrite").save(hdfs + "user/hdfs/employee/details.csv")

 // Writing file in PARQUET format
 df.write.format("parquet").mode("overwrite").save(hdfs + "user/hdfs/employee/details")

 //  Reading CSV files from HDFS
 val dfIncsv = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", "true").load(hdfs + "user/hdfs/employee/details.csv")

 // Reading PQRQUET files from HDFS
 val dfInParquet = sqlContext.read.parquet(hdfs + "user/hdfs/employee/details")