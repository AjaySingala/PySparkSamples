// readjson.scala
//package com.sparkbyexamples.spark.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object FromJsonFile {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[3]")
      .appName("AjaySingala")
      .getOrCreate()
    val sc = spark.sparkContext

    //read json file into dataframe
    val df = spark.read.json("../resources/employee.json")
    df.printSchema()
    df.show(false)

    //// Read from local.
    // val df = spark.read.json("file:///home/maria_dev/files/employee.json")

    //read multiline json file
    val multiline_df = spark.read
      .option("multiline", "true")
      .json("../resources/employee_m.json")
    multiline_df.show(false)

    //read multiple files
    val df2 = spark.read.json("../resources/e1.json", "../resources/e2.json")
    df2.show(false)

    // //read files from a folder.
    // val df2 = spark.read.json("somefolder")
    // df2.show(false)

    //Define custom schema
    val schema = new StructType()
      .add("RecordNumber", IntegerType, true)
      .add("Zipcode", IntegerType, true)
      .add("ZipCodeType", StringType, true)
      .add("City", StringType, true)
      .add("State", StringType, true)
      .add("LocationType", StringType, true)
      .add("Lat", DoubleType, true)
      .add("Long", DoubleType, true)
      .add("Xaxis", IntegerType, true)
      .add("Yaxis", DoubleType, true)
      .add("Zaxis", DoubleType, true)
      .add("WorldRegion", StringType, true)
      .add("Country", StringType, true)
      .add("LocationText", StringType, true)
      .add("Location", StringType, true)
      .add("Decommisioned", BooleanType, true)
      .add("TaxReturnsFiled", StringType, true)
      .add("EstimatedPopulation", IntegerType, true)
      .add("TotalWages", IntegerType, true)
      .add("Notes", StringType, true)

    val df_with_schema = spark.read
      .schema(schema)
      .json("../resources/zipcodes.json")
    df_with_schema.printSchema()
    df_with_schema.show(false)

    // Read JSON file using Spark SQL.
    spark.sqlContext.sql(
      "CREATE TEMPORARY VIEW zipcode USING json OPTIONS" + " (path '../resources/zipcodes.json')"
    )
    spark.sqlContext.sql("select * from zipcodes").show(false)

    // Write Spark DataFrame to JSON file.
    df2.write.json("/tmp/spark_output/zipcodes.json")
  }
}
