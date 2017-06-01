package test.kafka.partitioning

import java.util
import java.util.Properties

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable

/**
  * Created by sergey on 5/24/17.
  */
class Consumer(name: String, topic: String) {
  def consume: Unit = {
    val consumer = getConsumer()
    val lastOffsets = mutable.HashMap.empty[Int, Long]

    consumer.subscribe(util.Collections.singletonList(topic), new ConsumerRebalanceListener {
      override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
        println("consumer: " + name + ": onPartitionsAssigned with: " + partitions)

        partitions.asScala.foreach(partition => {
          lastOffsets += (partition.partition() -> (consumer.position(partition)))
          println("consumer: " + name + ", topic: " + partition.topic() + ", partition: " + partition.partition() + ", offset:" +  consumer.position(partition))
        })

        consumer.seekToBeginning(partitions)
      }

      override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {
        println("consumer: " + name + ": onPartitionsRevoked with: " + partitions)
      }
    })

    println("consumer: " + name + " started listening")
    while(true) {
        val inputRecords = consumer.poll(1000)
        for (record <- inputRecords) {
          var action = "store"
          if (record.offset() >= lastOffsets(record.partition())) action = "process"
          println("consumer: " + name + ", partition: " + record.partition() + ", offset: " + record.offset() + ", value: " + record.value() + " action: " + action)
        }

    }
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

object Consumer {
  def main(args: Array[String]): Unit = {
    new Consumer("0", "test-1").consume
  }
}