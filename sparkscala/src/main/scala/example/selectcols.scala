// selectcols.scala
package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object SelectCols {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[1]")
      .appName("AjaySingala")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext

    val data = Seq(
      ("James", "Smith", "USA", "CA"),
      ("Michael", "Rose", "USA", "NY"),
      ("Robert", "Williams", "USA", "CA"),
      ("Maria", "Jones", "USA", "FL")
    )
    val columns = Seq("firstname", "lastname", "country", "state")
    import spark.implicits._
    val df = data.toDF(columns: _*)
    df.show(false)

    // select single or multiple columns columns from DataFrame.
    df.select("firstname", "lastname").show()

    //Using Dataframe object name
    df.select(df("firstname"), df("lastname")).show()

    //Using col function, use alias() to get alias name
    import org.apache.spark.sql.functions.col
    df.select(col("firstname").alias("fname"), col("lastname")).show()

    // Select All Columns.
    //Show all columns from DataFrame
    df.select("*").show()
    val columnsAll = df.columns.map(m => col(m))
    df.select(columnsAll: _*).show()
    df.select(columns.map(m => col(m)): _*).show()

    // Select Columns from List.
    val listCols = List("lastname", "country")
    df.select(listCols.map(m => col(m)): _*).show()

    // Select First N Columns.
    // Select Column By Position or Index//Select first 3 columns.
    df.select(df.columns.slice(0, 3).map(m => col(m)): _*).show()

    // Select Column By Position or Index//Select first 3 columns.
    //Selects 4th column (index starts from zero)
    df.select(df.columns(3)).show()
    //Selects columns from index 2 to 4
    df.select(df.columns.slice(2, 4).map(m => col(m)): _*).show()

    // Select Nested Struct Columns.
    //Show Nested columns
    import org.apache.spark.sql.types.{StringType, StructType}
    val data2 = Seq(
      Row(Row("James", "", "Smith"), "OH", "M"),
      Row(Row("Anna", "Rose", ""), "NY", "F"),
      Row(Row("Julia", "", "Williams"), "OH", "F"),
      Row(Row("Maria", "Anne", "Jones"), "NY", "M"),
      Row(Row("Jen", "Mary", "Brown"), "NY", "M"),
      Row(Row("Mike", "Mary", "Williams"), "OH", "M")
    )

    val schema = new StructType()
      .add(
        "name",
        new StructType()
          .add("firstname", StringType)
          .add("middlename", StringType)
          .add("lastname", StringType)
      )
      .add("state", StringType)
      .add("gender", StringType)

    val df2 =
      spark.createDataFrame(spark.sparkContext.parallelize(data2), schema)
    df2.printSchema()
    df2.show(false)

    df2.select("name").show(false)
    // to get the specific column from a struct, you need to explicitly qualify.
    df2.select("name.firstname","name.lastname").show(false)
    // to get all columns from struct column.
    df2.select("name.*").show(false)

    
  }
}
