// OrderProducer.scala
// spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0,net.liftweb:lift-json_2.11:3.5.0  ~/kafkaspark_2.11-0.1.0-SNAPSHOT.jar --class eCommerce.OrderProducer

// On Custom VM:
// kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic order_records
// kafka-console-producer.sh --broker-list localhost:9092 --topic order_records
// kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic order_records

// On Hortonworks VM:
// $KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper sandbox-hdp.hortonworks.com:2181  --replication-factor 1 --partitions 1 --topic order_records
// $KAFKA_HOME/bin/kafka-console-producer.sh --broker-list sandbox-hdp.hortonworks.com:6667  --topic order_records
// $KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server sandbox-hdp.hortonworks.com:6667  --topic order_records
// $KAFKA_HOME/bin/kafka-topics.sh --delete --zookeeper sandbox-hdp.hortonworks.com:2181  --topic order_records

package eCommerce

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import net.liftweb.json._
import net.liftweb.json.Serialization.write

object OrderProducer {
    def main_op(args: Array[String]) {
        println(s"Running the Order Producer...")

        val props: Properties = new Properties()
        //props.put("bootstrap.servers","localhost:9092")
        props.put("bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
        props.put(
            "key.serializer",
            "org.apache.kafka.common.serialization.StringSerializer"
        )
        props.put(
            "value.serializer",
            "org.apache.kafka.common.serialization.StringSerializer"
        )

        val producer = new KafkaProducer[String, String](props)
        val topic = "order_records"        
        try {
            for (i <- 0 to 100) {
                println(s"Running cycle # $i...")
                val orders = OrderGenerator.generateRecords(50).toList
                implicit val formats = DefaultFormats

                orders.map(o => {
                    val jsonString = write(o)
                    println(jsonString)
                    val json = parse(jsonString)
                    val record = new ProducerRecord[String, String](topic, jsonString)

                    val metadata = producer.send(record)
                    // printf(
                    // s"sent record(key=%s value=%s) " +
                    //     "meta(partition=%d, offset=%d)\n",
                    // record.key(),
                    // record.value(),
                    // metadata.get().partition(),
                    // metadata.get().offset()
                    // )

                })
                
                println(s"Completed cycle # $i...")
                if(i < 100) {
                    val ms = 10000
                    println(s"Going to sleep for $ms milliseconds...")
                    Thread.sleep(ms)
                }
            }
        } catch {
            case e: Exception => e.printStackTrace()
        } finally {
            producer.close()
        }
    }
}
