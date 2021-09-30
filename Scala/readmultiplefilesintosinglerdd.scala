// readmultiplefilesintosinglerdd.scala
package com.examples.spark.rdd

import org.apache.spark.sql.SparkSession

object ReadMultipleFiles extends App {

  val spark:SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("ajaysingala.com")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  println("read all text files from a directory to single RDD")
  val rdd = spark.sparkContext.textFile("../resources/csv/*")
  rdd.foreach(f=>{
    println(f)
  })

  println("read text files base on wildcard character")
  val rdd2 = spark.sparkContext.textFile("../resources/csv/text*.txt")
  rdd2.foreach(f=>{
    println(f)
  })

  println("read multiple text files into a RDD")
  val rdd3 = spark.sparkContext.textFile("../resources/csv/text01.txt,../resources/csv/text02.txt")
  rdd3.foreach(f=>{
    println(f)
  })

  println("Read files and directory together")
  val rdd4 = spark.sparkContext.textFile("../resources/csv/text01.txt,../resources/csv/text02.txt,../resources/csv/*")
  rdd4.foreach(f=>{
    println(f)
  })


  val rddWhole = spark.sparkContext.wholeTextFiles("../resources/csv/*")
  rddWhole.foreach(f=>{
    println(f._1+"=>"+f._2)
  })

  val rdd5 = spark.sparkContext.textFile("../resources/csv/*")
  val rdd6 = rdd5.map(f=>{
    f.split(",")
  })

  rdd6.foreach(f => {
    println("Col1:"+f(0)+",Col2:"+f(1))
  })

}

