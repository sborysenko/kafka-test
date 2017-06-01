package test.kafka.partitioning

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Created by sergey on 6/1/17.
  */
class SimpleConsumer(name: String) {
  def consume: Unit = {
    val consumer = getConsumer()
    consumer.subscribe(util.Collections.singletonList("test-1"), new ConsumerRebalanceListener {
      override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
        val thread = new Thread() {
          override def run(): Unit = {
            println(name + ": onPartitionsAssigned with: " + partitions)

            var offsets = mutable.HashMap.empty[Int, Long]
            partitions.asScala.foreach(partition => {
              offsets += (partition.partition() -> (consumer.position(partition)))
              println(name + ": " + partition.topic() + ":" + partition.partition() + ":" +  consumer.position(partition))
            })

            consumer.seekToBeginning(partitions)

            while (true) {
              val inputRecords = consumer.poll(1000)
              val iterator = inputRecords.iterator()
              for (record <- inputRecords) {
                var action = "store"
                if (record.offset() >= offsets(record.partition())) action = "process"
                println(name + ": " + record.partition() + ": " + record.offset() + ": " + record.value() + " -> " + action)
              }
            }
          }
        }
        thread.start()
      }

      override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {
        println(name + ": onPartitionsRevoked with: " + partitions)
      }
    })
    consumer.poll(0)
  }

  def getConsumer(): KafkaConsumer[String, String] ={
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("group.id", "test-group")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("session.timeout.ms", "30000")

    new KafkaConsumer[String, String](props)
  }
}
