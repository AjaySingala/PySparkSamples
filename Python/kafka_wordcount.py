#kafka_wordcount.py
"""
 Counts words in UTF8 encoded, '\n' delimited text received from the network every second.
 Usage: kafka_wordcount.py <zk> <topic>
 To run this on your local machine, you need to setup Kafka and create a producer first, see
 http://kafka.apache.org/documentation.html#quickstart
 and then run the example
    `$ bin/spark-submit --jars \
      external/kafka-assembly/target/scala-*/spark-streaming-kafka-assembly-*.jar \
      examples/src/main/python/streaming/kafka_wordcount.py \
      localhost:2181 test`
      
    $ $KAFKA_HOME/bin/kafka-console-producer.sh --brokerst sandbox-hdp.hortonworks.com:6667 --topic test
    $ $KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server sandbox-hdp.hortonworks.com:6667 --topic test --from-beginning
    $ spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8-assembly_2.11:2.4.4 kafka_wordcount.py  localhost:2181 test

"""
from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka_wordcount.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonStreamingKafkaWordCount")
    ssc = StreamingContext(sc, 10)

    zkQuorum, topic = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
    lines = kvs.map(lambda x: x[1])
    counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b)
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()
