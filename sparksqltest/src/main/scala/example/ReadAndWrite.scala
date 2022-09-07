// ReadAndWrite.scala
// spark-submit sparkscala_2.11-0.1.0-SNAPSHOT.jar  --class example.ReadAndWrite

package example

import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object ReadAndWrite {

  // function to get parquet dataset from a file.
  def readParquetFile(spark: SparkSession,
                      locationPath: String): DataFrame = {
    spark.read
      .parquet(locationPath)
  }

  // function to get text dataset from a file.
  def readTextfileToDataSet(spark: SparkSession,
                            locationPath: String): Dataset[String] = {
    spark.read
      .textFile(locationPath)
  }

  // function to write RDD as Text File in a file.
  def writeDataframeAsCSV(df: DataFrame,
                          locationPath: String,
                          oDate: String,
                          delimiter: String,
                          header: String
                         ): Unit = {
    df.write
      .option("delimiter", delimiter)
      .option("header", header)
      .format("csv")
      .save(locationPath + "/" + oDate + "/")
  }

}
