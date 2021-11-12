// SparkStreamKafkaSource.scala
package example

// kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic testscala
// kafka-console-producer --broker-list localhost:9092 --topic testscala
// kafka-console-consumer --bootstrap-server localhost:9092 --topic testscala

// Import Libraries
import org.apache.spark.sql.SparkSession

object SparkStreamKafkaSource {
  def main(args: Array[String]): Unit = {
    // Create streaming DF.
    val initDf = spark.readStream
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
