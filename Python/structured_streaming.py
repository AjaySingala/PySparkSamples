from pyspark import SparkContext
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("ajaysingala")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    df = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "sandbox-hdp-hortonworks.com:6667")
        .option("subscribe", "json_topic")
        .option("startingOffsets", "earliest")
        .load()

    df.printSchema()

    schema = new StructType()
      .add("id",IntegerType)
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType)
      .add("dob_year",IntegerType)
      .add("dob_month",IntegerType)
      .add("gender",StringType)
      .add("salary",IntegerType)

    person = df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).as("data"))
      .select("data.*")

    df.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .outputMode("append")
      .option("kafka.bootstrap.servers", " localhost:9092")
      .option("topic", "josn_topic")
      .start()
      .awaitTermination()

