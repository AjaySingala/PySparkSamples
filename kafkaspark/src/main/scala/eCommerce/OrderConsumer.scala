// OrderConsumer.scala
// spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0,net.liftweb:lift-json_2.11:3.5.0  ~/kafkaspark_2.11-0.1.0-SNAPSHOT.jar --class eCommerce.OrderConsumer

// On Custom VM:
// kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic order_records
// kafka-console-producer.sh --broker-list localhost:9092 --topic order_records
// kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic order_records

// On Hortonworks VM:
// $KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper sandbox-hdp.hortonworks.com:2181  --replication-factor 1 --partitions 1 --topic order_records
// $KAFKA_HOME/bin/kafka-console-producer.sh --broker-list sandbox-hdp.hortonworks.com:6667  --topic order_records
// $KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server sandbox-hdp.hortonworks.com:6667  --topic order_records
// $KAFKA_HOME/bin/kafka-topics.sh --delete --zookeeper sandbox-hdp.hortonworks.com:2181  --topic order_records

/*
Sample JSON:
{
  "transactionTimestamp": "2022-05-03T18:23:44Z",
  "order_id": "GBIU5S2XWO",
  "datetime": "2022-05-03 18:23",
  "customer_id": 5,
  "customer_name": "Chris",
  "product_id": 3,
  "product_name": "Pens",
  "product_category": "Stationery",
  "qty": 1,
  "price": 1.99,
  "amount": 1.99,
  "country": "FL",
  "city": "Orlando",
  "ecommerce_website_name": "target.com",
  "payment_type": "Bank Transfer",
  "payment_txn_id": "SMRLMG1AO1",
  "payment_txn_success": "N",
  "failure_reason": ""
}
*/

package eCommerce

import org.apache.spark.sql.{Column, Row, SparkSession, Dataset}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.ForeachWriter

object OrderConsumer {
    def main(args: Array[String]) {
        println("Running OrderConsumer...")

        println("Creating Spark Session...")
        val spark = SparkSession.builder
        .appName("OrderConsumer")
        .config("spark.master", "local[*]")
        .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        println("Define our input stream...")
        // val kafkaEndpoint = "localhost:9092"                 // Default.
        val kafkaEndpoint = "sandbox-hdp.hortonworks.com:6667"  // Hortonworks.

        val schema = StructType(
            List(
                StructField("order_id", StringType, true),          
                StructField("customer_id", IntegerType, true),
                StructField("customer_name", StringType, true),
                StructField("product_id", IntegerType, true),
                StructField("product_name", StringType, true),
                StructField("product_category", StringType, true),
                StructField("payment_type", StringType, true),
                StructField("qty", IntegerType, true),
                StructField("price", DoubleType, true),
                StructField("amount", DoubleType, true),
                StructField("datetime", StringType, true),
                StructField("country", StringType, true),
                StructField("city", StringType, true),
                StructField("ecommerce_website_name", StringType, true),
                StructField("payment_txn_id", StringType, true),
                StructField("payment_txn_success", StringType, true),
                StructField("transactionTimestamp", TimestampType, true)
            )
        )

        val inputStream = spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", kafkaEndpoint)
            .option("auto.offset.reset", "latest")
            .option("value.deserializer", "StringDeserializer")
            .option("subscribe", "order_records")
            .load()

        println("The schema from the stream dataframe...")
        inputStream.printSchema()

        println("Convert Kafka topic input stream value, that has a binary type, into a meaningful dataframe...")
        val initial = inputStream.selectExpr("CAST(value AS STRING)").toDF("value")
        initial.printSchema()

        println("Convert this String, into its JSON representation...")
        val aggregation_one = initial.select(from_json(col("value"), schema))
        aggregation_one.printSchema()

        println("Select the embedded values...")
        val aggregation = initial
        .select(from_json(col("value"), schema).alias("tmp"))
        .select("tmp.*")
        aggregation.printSchema()

        // println("Write the full data into a sink...")
        // val dfOut = aggregation.writeStream
        // .outputMode("update")
        // .option("truncate", false)
        // .format("console")
        // .start()
        // // dfOut.awaitTermination()      

        // // with window().
        // val windows = aggregation
        //     // .withWatermark("transactionTimestamp", "5 minutes")
        //     .withWatermark("transactionTimestamp", "1 minute")
        //     .groupBy(
        //         // window(col("transactionTimestamp"), "1 minute", "1 minute"),
        //         //window(col("transactionTimestamp"), "5 seconds", "5 seconds"),
        //         col("city")
        //     )        

        // println("Create a dataframe with multiple aggregations...")
        // import org.apache.spark.sql.functions._
        // val aggregatedDF = windows.agg(sum("amount").alias("amount"), count("*").alias("count"))

        // // Without window().
        // val aggregatedDF = aggregation
        //     .withWatermark("transactionTimestamp", "1 minutes")
        //     .groupBy(col("city"), col("transactionTimestamp"))
        //     .agg(sum("amount").alias("amount"), count("*").alias("count"))
  

        // println("Write the aggregated data into a sink...")
        // val dfcount = aggregatedDF.writeStream
        //     .outputMode("update")
        //     .option("truncate", false)
        //     .format("console")
        //     .start()
        // // dfcount.awaitTermination()        

        // Group By PaymentType, Sum Amount.
        val windowsPaymentType = aggregation
            //.withWatermark("transactionTimestamp", "5 minutes")
            .withWatermark("transactionTimestamp", "1 minute")
            .groupBy(
                //window(col("transactionTimestamp"), "1 minute", "1 minute"),
                window(col("transactionTimestamp"), "5 seconds", "5 seconds"),
                col("payment_type")
            )        

        // Write to console.
        import org.apache.spark.sql.functions._
        val dfPaymentTypeAmount = windowsPaymentType.agg(sum("amount").alias("amount"))
        val dfPaymentTypeAmountQuery = dfPaymentTypeAmount.writeStream
            .outputMode("update")
            .option("truncate", false)
            .format("console")
        //     .start()

        // Write to HDFS as json files.
        val dfPaymentTypeAmountQueryJson = dfPaymentTypeAmount.writeStream.format("json")
            .option("path", "/tmp/output/orders/payment_type/json")
            .option("checkpointLocation","/tmp/output/orders/payment_type/json_checkpoint")
            .outputMode("append").start()
        
        // // Aggregate (without watermark and window) and write to sinks.
        // // Append output mode not supported when there are streaming aggregations on 
        // // streaming DataFrames/DataSets without watermark.
        // println("Create a DF calculate sum and max of amount grouped by city (without WaterMark)...")
        // val dfSumAmountByCity = aggregation
        //     .groupBy("city")
        //     .agg(
        //         sum("amount").alias("TotalAmount"),
        //         max("amount").alias("MaxAmount")
        //     )

        // // This will work w/o withWatermark as it is "update" mode.
        // println("Writing Sum of Amount By City to console...")
        // val q1 = dfSumAmountByCity.writeStream.format("console").outputMode("update").option("truncate", false).start()

        // // Writing to another console also works.
        // //val q2 = dfSumAmountByCity.writeStream.format("console").outputMode("update").option("truncate", true).start()

        // // Append output mode not supported when there are streaming aggregations on 
        // // streaming DataFrames/DataSets without watermark.
        // // So, have to use withWatermark() to get the following to work.
        // println("Writing Aggregated Sum of Amount By City to parquet file (DF with WaterMark )...")
        // val q3 = dfSumAmountByCity.writeStream.format("parquet").option("path", "/tmp/output/orders/parquet")
        //     .option("checkpointLocation","/tmp/output/orders/parquet_checkpoint")
        //     .outputMode("append").start()

        // // Same here.
        // println("Writing Aggregated Sum of Amount By City to json file (DF with WaterMark )...")
        // val q4 = dfSumAmountByCity.writeStream.format("json").option("path", "/tmp/output/orders/json")
        //     .option("checkpointLocation","/tmp/output/orders/json_checkpoint")
        //     .outputMode("append").start()

        spark.streams.awaitAnyTermination()

        // // Using foreach(), process each row.
        // val customWriter = new ForeachWriter[Row] {
        //     override def open(partitionId: Long, version :Long) = true
        //     override def process(value: Row) = {
        //         print(s"${value.getAs("city")} : ")
        //         print(s"${value.getAs("TotalAmount")}")
        //         println()
        //     }

        //     override def close(errorOrNull: Throwable) = {}
        // }

        // dfSumAmountByCity
        //     .writeStream
        //     .outputMode("update")
        //     .foreach(customWriter)
        //     .start()
        //     .awaitTermination()

        // // Use foreachBatch to write to multiple sinks.
        // // Works only in Spark 2.4.0 and above.
        // def saveToMultipleSinks = (df: Dataset[Row], batchId: Long) => {
        //     df
        //         //.withColumn("batchId", date_format(current_date(),"yyyyMMdd"))
        //         .withColumn("batchId", lit(batchId))
        //         .write.format("csv")
        //         .option("path", "/tmp/output/orders/csv/batch_${batchId}")
        //         .mode("append")
        //         .save()

        //     df
        //         .write.mode("overwrite")
        //         .json(s"/tmp/output/orders/json/batch_${batchId}")

        //     df
        //         .write.mode("overwrite")
        //         .parquet(s"/tmp/output/orders/parquet/batch_${batchId}")
        // }

        // dfSumAmountByCity
        //     .writeStream
        //     .outputMode("append")
        //     .foreachBatch(saveToMultipleSinks)
        //     .start()
        //     .awaitTermination()
    }

}