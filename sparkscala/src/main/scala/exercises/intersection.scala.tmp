package exercises

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object exr_intersection {
  def main_FromCSVFile(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("AjaySingala")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val sc = spark.sparkContext

    val parallel = sc.parallelize(1 to 9)
    val par2 = sc.parallelize(5 to 15)
    val par3 = parallel.intersection(par2).collect
    par3.foreach(println)
  }
}
