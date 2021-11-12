// SparkStreamFileSource.scala
package example

// Copy these files in the sequence below from data/stocks to data/stream to simulate streaming.
// 1.	MSFT_2017.csv
// 2.	GOOGL_2017.csv
// 3.	MSFT_2016.csv
// 4.	AMZN_2017.csv

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.types._

object SparkStreamFileSource {
  def main(args: Array[String]): Unit = {

    // Create Spark Session
    // val spark = SparkSession
    //   .builder()
    //   .master("local")
    //   .appName("File Source")
    //   .getOrCreate()
    val spark = spark.builder.appName("File Source").config("spark.master", "local[*]").getOrCreate()

    // Set Spark logging level to ERROR.
    spark.sparkContext.setLogLevel("ERROR")

    // Define Schema
    val schema = StructType(
      List(
        StructField("Date", StringType, true),
        StructField("Open", DoubleType, true),
        StructField("High", DoubleType, true),
        StructField("Low", DoubleType, true),
        StructField("Close", DoubleType, true),
        StructField("Adjusted Close", DoubleType, true),
        StructField("Volume", DoubleType, true)
      )
    )

    // Extract the Name of the stock from the file name.
    def getFileName: Column = {
      val file_name = reverse(split(input_file_name(), "/")).getItem(0)
      split(file_name, "_").getItem(0)
    }

    // Create Streaming DataFrame by reading data from File Source.
    val initDF = (
      spark.readStream
        .format("csv")
        // This will read maximum of 2 files per mini batch. However, it can read less than 2 files.
        .option("maxFilesPerTrigger", 2)
        .option("header", true)
        .option("path", "file:///home/hdoop/SparkSamples/data/stream")
        .schema(schema)
        .load()
        .withColumn(
          "Name",
          getFileName
        )
    )

    // Uncomment this line and comment the below view and query statements.
    // Basic Transformation.
    val stockDf = initDF
      .groupBy(col("Name"), year(col("Date")).as("Year"))
      .agg(max("High").as("Max"))

    //// Comment above line to use this. And uncomment these.
    // // Register DataFrame as view.
    // initDF.createOrReplaceTempView("stockView")

    // // Run SQL Query
    // val query =
    //   """select year(Date) as Year, Name, max(High) as Max from stockView group by Name, Year"""
    // val stockDf = spark.sql(query)

    // Output to Console
    stockDf.writeStream
      .outputMode("update") // Try "update" and "complete" mode.
      .option("truncate", false)
      .option("numRows", 3)
      .format("console")
      .start()
      .awaitTermination()

  }
}
