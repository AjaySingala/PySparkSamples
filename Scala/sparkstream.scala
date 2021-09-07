// sparkstream.scala - kafkaUtils does not work.

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka


class SparkObjects() {

  def GetSparkConf(): SparkConf ={
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("NetworkWordCount")
    conf
  }

  def GetStreamingContext(): StreamingContext ={
    new StreamingContext(GetSparkConf(), Seconds(10))
  }
}


object SparkKafkaConnection {
  def main(args: Array[String]): Unit = {
    val ssc = new SparkObjects().GetStreamingContext()
    val kafkaStream = kafka.KafkaUtils.createStream(ssc, "localhost:2181","spark-streaming-consumer-group", Map("test" -> 5) )
    kafkaStream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
