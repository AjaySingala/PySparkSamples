// SparkStreamRateSource.scala

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._

object SparkStreamRateSource {
  def main(args: Array[String]): Unit = {

    // Create Spark Session
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Rate Source")
      .getOrCreate()

    // Set Spark logging level to ERROR to avoid various other logs on console.
    spark.sparkContext.setLogLevel("ERROR")

    // Create streaming DataFrame.
    val initDF = (spark.readStream
      .format("rate")
      .option("rowsPerSecond", 1)
      .load())

    println("Streaming DataFrame : " + initDF.isStreaming)

    // Basic transformation: generate another column result by just adding 1 to column value
    val resultDF = initDF
      .withColumn("result", col("value") + lit(1))

    // Output to console.
    resultDF.writeStream
      .outputMode("append")
      .option("truncate", false)
      .format("console")
      .start()
      .awaitTermination()

    // Use df.writeStream.option(“numRows”,20) to display more than 20 rows (which is the default).
  }
}
