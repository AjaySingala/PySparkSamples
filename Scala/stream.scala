import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

//object SparkStreamingConsumerKafkaJson {

//  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local[3]").appName("ajaysingala").getOrCreate()
    
    import spark.implicits._
    
    spark.sparkContext.setLogLevel("ERROR")

    // Read from start.
    val df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667").option("subscribe", "json_topic").option("startingOffsets", "earliest").load()

    //df.printSchema()

    val schema = new StructType().add("id",IntegerType).add("firstname",StringType).add("middlename",StringType).add("lastname",StringType).add("dob_year",IntegerType).add("dob_month",IntegerType).add("gender",StringType).add("salary",IntegerType)

    // Since the value is in binary, first we need to convert the binary value to String using selectExpr().
    // Also apply the schema.
    val personDF = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).as("data")).select("data.*")

    /**
     *uncomment below code if you want to write it to console for testing.
     */
   val query = personDF.writeStream.format("console").outputMode("append").option("truncate", "false").start()
      //.awaitTermination()


    /**
      *uncomment below code if you want to write it to kafka topic.
      */
   val dfOut = personDF.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value").alias("value")
   dfOut.printSchema()
   dfOut
      .writeStream.format("kafka")
      .outputMode("append")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("checkpointLocation", "/tmp/kafka_checkpoints")
      .option("topic", "json_output_topic")
      .start()
      //.awaitTermination()

   dfOut.createOrReplaceTempView("Person")
   val dfSQL =  spark.sql("SELECT * FROM PERSON")
   val dfJSON = dfSQL.toJSON
   dfJSON.writeStream.format("console").outputMode("append").option("truncate", "false").start()
//  }
//}
