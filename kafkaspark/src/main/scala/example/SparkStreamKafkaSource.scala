// SparkStreamKafkaSource.scala
// spark-submit kafkaspark_2.11-0.1.0-SNAPSHOT.jar --class example.SparkStreamKafkaSource
package example

// kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic testscala
// kafka-console-producer --broker-list localhost:9092 --topic testscala
// kafka-console-consumer --bootstrap-server localhost:9092 --topic testscala

// Import Libraries
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

object SparkStreamKafkaSource {
  def main_k(args: Array[String]): Unit = {
    // Create Spark Session
    //val sparkSession = SparkSession
    //  .builder()
    //  .master("local")
    //  .appName("Kafka Source")
    //  .getOrCreate()

    val spark = SparkSession.builder.appName("Kafka Source").config("spark.master", "local[*]").getOrCreate()

    import spark.implicits._
    // Set Spark logging level to ERROR.
    spark.sparkContext.setLogLevel("ERROR")
    // Create streaming DF.
    val initDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .load()
      .select(col("value").cast("string"))

    // Transformation.
    val wordCount = initDF
      .select(explode(split(col("value"), " ")).alias("words"))
      .groupBy("words")
      .count()

    // Output to console.
    wordCount.writeStream
      .outputMode("update")
      .format("console")
      .start()
      .awaitTermination()

  }
}
