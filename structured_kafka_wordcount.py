import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("""
        Usage: structured_kafka_wordcount.py <bootstrap-servers> <subscribe-type> <topics>
        """, file=sys.stderr)
        sys.exit(-1)

    bootstrapServers = sys.argv[1]
    subscribeType = sys.argv[2]
    topics = sys.argv[3]

    spark = SparkSession.builder.appName("StructuredKafkaWordCount").getOrCreate()

    # Create DataSet representing the stream of input lines from kafka
    #lines = spark.readStream.format("kafka").option("kafka.bootstrap.servers", bootstrapServers).option(subscribeType, topics).load().selectExpr("CAST(value AS STRING)")
    lines = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "test").load()
    lines.printSchema()
 
    lines.selectExpr("CAST(value AS STRING)")

    # Split the lines into words
    words = lines.select(
        # explode turns each item in an array into a separate row
        explode(
            split(lines.value, ' ')
        ).alias('word')
    )

    # Generate running word count
    wordCounts = words.groupBy('word').count()

    # Start running the query that prints the running counts to the console
    query = wordCounts.writeStream.outputMode('complete').format('console').start()

    query.awaitTermination()
