# Provides findspark.init() to make pyspark importable as a regular library.
#import findspark
#findspark.init()

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import SparkContext
from pyspark.streaming.kafka import KafkaUtils
import json
import time

if __name__ == "__main__":
    #spark = SparkSession.builder.master("local").appName("Kafka-Spark ISS demo").getOrCreate()

    sc = SparkContext(appName="Kafka-Spark ISS demo")
    ssc = StreamingContext(sc, 20)

    message = KafkaUtils.createDirectStream(ssc, topics = ["issTopic"], kafkaParams = {"metadata.broker.list": "sandbox-hdp.hortonworks.com:6667"})

    data = message.map(lambda x: x[1])

    def functoRDD(rdd):
        try:
            rdd1 = rdd.map(lambda x: json.loads(x))
            df = spark.read.json(rdd1)
            df.show()

            df.createOrReplaceTempView("issView")
            df1 = spark.sql("SELECT iss_position.latitude, iss_position.longitude, message, timestamp from issView")

            # Write to CSV file.
            df1.write.format("csv").mode("append").save("isspath")

        except:
            pass

    data.foreach(functoRDD)

    # words = message.mapo(lambda x: x[1]).flatMap(lambda x: x.split(" "))
    # wordCount = words.map(lambda x: (x,1)).reduceByKey(lambda a, b: a+b)
    # wordCount.pprint()

    ssc.start()
    ssc.awaitTermination()
