// flatMap.scala
package exercises

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object exr_flatMap {
  def main_FromCSVFile(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("AjaySingala")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val sc = spark.sparkContext

    // Read CSV file.
    println("\nRead Baby names csv file...")
    val babyNames = sc.textFile("file:///home/maria_dev/SparkSamples/resources/baby_names.csv")

    // Iterates over every line in the babyNames RDD (originally created from baby_names.csv file) and 
    // splits into new RDD of Arrays of Strings.
    val rows = babyNames.flatMap(line => line.split(","))
    rows.foreach(println)
  }
}
