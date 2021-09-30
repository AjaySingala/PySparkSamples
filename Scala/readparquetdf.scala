// readparquetdf.scala

// package com.sparkbyexamples.spark.dataframe

import org.apache.spark.sql.SparkSession

object ParquetExample {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[1]")
      .appName("AjaySingala.com")
      .getOrCreate()

    //  Create a Spark DataFrame from Seq object.
    val data = Seq(
      ("James ", "", "Smith", "36636", "M", 3000),
      ("Michael ", "Rose", "", "40288", "M", 4000),
      ("Robert ", "", "Williams", "42114", "M", 4000),
      ("Maria ", "Anne", "Jones", "39192", "F", 4000),
      ("Jen", "Mary", "Brown", "", "F", -1)
    )

    val columns =
      Seq("firstname", "middlename", "lastname", "dob", "gender", "salary")

    import spark.sqlContext.implicits._
    val df = data.toDF(columns: _*)
    df.show()
    df.printSchema()

    // Write DataFrame to Parquet file format.
    df.write.parquet("/tmp/output/people.parquet")

    // Read Parquet file into DataFrame.
    val parqDF = spark.read.parquet("/tmp/output/people.parquet")

    // Append to existing Parquet file.
    df.write.mode("append").parquet("/tmp/output/people.parquet")

    // Using SQL queries on Parquet.
    parqDF.createOrReplaceTempView("ParquetTable")
    spark.sql("select * from ParquetTable where salary >= 4000").explain()
    val parkSQL = spark.sql("select * from ParquetTable where salary >= 4000 ")
    parkSQL.show()
    parkSQL.printSchema()

    // Spark parquet partition – Improving performance.
    df.write
      .partitionBy("gender", "salary")
      .parquet("/tmp/output/people2.parquet")

    // Execution of this query is significantly faster than the query without partition.
    // It filters the data first on gender and then applies filters on salary.
    val parqDF2 = spark.read.parquet("/tmp/output/people2.parquet")
    parqDF2.createOrReplaceTempView("ParquetTable2")
    val df3 =
      spark.sql(
        "select * from ParquetTable2 where gender='M' and salary >= 4000"
      )
    df3.explain()
    df3.printSchema()
    df3.show()

    // Read a specific Parquet partition.
    val parqDF3 = spark.read.parquet("/tmp/output/people2.parquet/gender=M")
  }
}
