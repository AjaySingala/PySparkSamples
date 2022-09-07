// ReadAndWriteTestSpec.scala
// run with "test" or ~testQuick on sbt prompt.
// sbt:sparksqltest> Test / testOnly example.ReadWriteTestSpec

package example

import org.apache.spark.sql
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import example.ReadAndWrite
import org.apache.spark.sql.DataFrame

case class Person(name: String, age: Int)

object Filters {
  def isSalaryMoreThan4000(dataset: DataFrame): DataFrame = {
    dataset.filter("salary > 4000")
  }

  def isSalaryLessThan4500(dataset: DataFrame): DataFrame = {
    dataset.filter("salary < 4500")
  }
}

class ReadAndWriteTestSpec extends FunSuite with BeforeAndAfterEach {

  private val master = "local"

  private val appName = "ReadAndWrite-Test"

  var spark : SparkSession = _
  // var resourcePath = "file:///Users/AjayJayantilalSingal/Big Data/SparkSamples/sparksqltest/src/test/resources"
  var resourcePath = "src/test/resources"

  override def beforeEach(): Unit = {
    spark = new sql.SparkSession.Builder().appName(appName).master(master).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  test("creating data frame from parquet file") {
    val sparkSession = spark
    import sparkSession.implicits._
    // val peopleDF = spark.read.json("src/test/resources/people.json")
    // peopleDF.write.mode(SaveMode.Overwrite).parquet("src/test/resources/people.parquet")
    val peopleDF = spark.read.json(resourcePath+"/people.json")
    peopleDF.write.mode(SaveMode.Overwrite).parquet(resourcePath+"/people.parquet")

    // val df = ReadAndWrite.readParquetFile(sparkSession,"src/test/resources/people.parquet")
    val df = ReadAndWrite.readParquetFile(sparkSession,resourcePath+"/people.parquet")
    df.printSchema()
  }

 test("Reading files of different format using readParquetFile should throw an exception") {
   intercept[Exception] {
     val sparkSession = spark

     val df = ReadAndWrite.readParquetFile(sparkSession,"src/test/resources/people.txt")
     df.printSchema()
   }
 }

  test("creating data frame from text file") {
    val sparkSession = spark
    import sparkSession.implicits._
    val peopleDF = ReadAndWrite.readTextfileToDataSet(sparkSession,"src/test/resources/people.txt").map(_.split(","))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
      .toDF()
    peopleDF.printSchema()
  }

  test("counts should match with number of records in a text file") {
    val sparkSession = spark
    import sparkSession.implicits._
    val peopleDF = ReadAndWrite.readTextfileToDataSet(sparkSession,"src/test/resources/people.txt").map(_.split(","))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
      .toDF()
    peopleDF.printSchema()

    assert(peopleDF.count() == 3)
  }

  test("data should match with sample records in a text file") {
    val sparkSession = spark
    import sparkSession.implicits._
    val peopleDF = ReadAndWrite.readTextfileToDataSet(sparkSession,"src/test/resources/people.txt").map(_.split(","))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
      .toDF()
    peopleDF.printSchema()

    assert(peopleDF.take(1)(0)(0).equals("Michael"))
  }

  test("Write a data frame as csv file") {
    val sparkSession = spark
    import sparkSession.implicits._
    val peopleDF = ReadAndWrite.readTextfileToDataSet(sparkSession,"src/test/resources/people.txt").map(_.split(","))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
      .toDF()

    //header argument should be boolean to the user to avoid confusions
    // java.time.Instant.now().toString, gives error On Windows due to ":" in filename.
    // ReadAndWrite.writeDataframeAsCSV(peopleDF,"src/test/resources/out.csv",java.time.Instant.now().toString,",","true")
    // Use random number instead.
    val r = scala.util.Random
    val num = r.nextInt().abs.toString()
    ReadAndWrite.writeDataframeAsCSV(peopleDF,"src/test/resources/out.csv",num,",","true")
  }

  test("Reading files of different format using readTextfileToDataSet should throw an exception") {
    intercept[org.apache.spark.sql.AnalysisException] {
    val sparkSession = spark

    import org.apache.spark.sql.functions.col

    val df = ReadAndWrite.readTextfileToDataSet(sparkSession,"src/test/resources/people.parquet")
    df.select(col("name"))

     }
  }

  test("Reading an invalid file location using readTextfileToDataSet should throw an exception") {
    intercept[Exception] {
      val sparkSession = spark

      import org.apache.spark.sql.functions.col

      val df = ReadAndWrite.readTextfileToDataSet(sparkSession,"src/test/resources/invalid.txt")
      df.show()

    }
  }

  test("Reading an invalid file location using readParquetFile should throw an exception") {
    intercept[Exception] {
      val sparkSession = spark

      val df = ReadAndWrite.readParquetFile(sparkSession,"src/test/resources/invalid.parquet")
      df.show()

    }
  }

  // Testing with isloation.
  // No isolation.
  test("filter salary > 4000") {
    val sparkSession = spark
    import sparkSession.implicits._

    val peopleDF = spark.read.json(resourcePath+"/people_filter.json")
    val filteredDF = peopleDF.filter("salary > 4000")

     filteredDF.show(false)
     assert(filteredDF.count() == 4)
  }

  // With isolation.
  test("filter salary > 4000 and < 4500") {
    val sparkSession = spark
    import sparkSession.implicits._

    val peopleDF = spark.read.json(resourcePath+"/people_filter.json")
    val filteredDF = Filters.isSalaryMoreThan4000(Filters.isSalaryLessThan4500(peopleDF))

     filteredDF.show(false)
     assert(filteredDF.count() == 2)
  }

  override def afterEach(): Unit = {
    spark.stop()
  }

}

