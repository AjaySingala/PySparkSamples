# Provides findspark.init() to make pyspark importable as a regular library.
#import findspark
#findspark.init()

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

#from pyspark.streaming import StreamingContext
#from pyspark.streaming.kafka import KafkaUtils
import json
import time

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Kafka-Spark ISS demo").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Create DataSet representing the stream of input lines from kafka
    message = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "issTopic").option("startingOffsets", "earliest").load()
    print("Printing schema...")
    message.printSchema()

    df = message.selectExpr("CAST(value as String)")
    df.printSchema()

    print("Trying this...")
    dft=df.withColumn("value",from_json(df.value,MapType(StringType(),StringType())))
    dft.printSchema()

    #print("Writing to console...")
    #df.writeStream.format("console").outputMode("append").start().awaitTermination()

    print(type(message))
    print(type(df))


    print("With schema....")
    schema = StructType([ \
        StructField("timestamp", StringType(), True), \
        StructField("message", StringType(), True), \
        StructField("iss_position", StructType([StructField("latitude", StringType(), True), StructField("longitude", StringType(), True)]), True)])
    dfData = df.withColumn("value", from_json("value", schema))
    dfData.printSchema()

    dfDataFormatted = dfData.select(col("value.timestamp").alias("timestamp"), \
        col("value.message").alias("message"), \
        col("value.iss_position.latitude").alias("latitude"), \
        col("value.iss_position.longitude").alias("longitude"))
    dfDataFormatted.printSchema()

    #dfDataFormatted.writeStream.format("console").outputMode("append").start().awaitTermination()

    print("Writing to topic iss_out_topic...")
    query = dfDataFormatted.select(to_json(struct("timestamp", "message", "latitude", "longitude")).alias("value")) \
        .writeStream\
        .outputMode("update")\
        .format("kafka")\
        .option("checkpointLocation", "/home/hdoop/spark_checkpoints")\
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .option("topic", "iss_out_topic") \
        .start() \
        .awaitTermination()

    #print("Create view....")
    #rawQuery = dfDataFormatted \
    #    .writeStream \
    #    .queryName("iss_view") \
    #    .format("memory") \
    #    .start() \

    #rawData = spark.sql("select * from iss_view")
    #rawData.show(truncate=False)

    @udf(returnType=StringType())
    def jsonparse(column, name):
        print("name:", name)
        print("column: ", str(json.loads(column)))
        if (name == "timestamp"):
            return str(json.loads(column)["timestamp"])
        if (name == "iss_position"):
            return str(json.loads(column)["iss_position"])
        if name == "latitude":
            return str(json.loads(column)["latitude"])
        if name == "longitude":
            return str(json.loads(column)["longitude"])

        #.withColumn("Latitude", jsonparse(col("json"), lit("latitude"))) \
        #.withColumn("timestamp", jsonparse(col("json"), lit("timestamp"))) \
    #df2 = message.selectExpr("CAST(value as String) as json") \
    #    .withColumn("timestamp", jsonparse(col("json"), lit("timestamp"))) \
    #    .withColumn("iss_position", jsonparse(col("json"), lit("iss_position"))) \
    #    .select("timestamp", "iss_position")
    #df2.printSchema()

    # Writing dataframe to console in append mode
    #query = df2 \
    #    .writeStream\
    #    .outputMode("append")\
    #    .format("console")\
    #    .start() \
    #    .awaitTermination()

    #df3 = df2.selectExpr("CAST(iss_position as String) as json2") \
    #    .withColumn("latitude", jsonparse(col("json2"), lit("latitude"))) \
    #    .select("latitude")
    #df3.printSchema()


    #rawQuery = message \
    #    .writeStream \
    #    .queryName("iss_view") \
    #    .format("memory") \
    #    .start()

    #data = spark.sql("SELECT * FROM iss_view")
    #data.show()

    #alertQuery = df \
    #    .writeStream \
    #    .queryName("iss_alerts")\
    #    .format("memory")\
    #    .start()

    #data2 = spark.sql("SELECT * FROM iss_alerts")
    #data2.show()
