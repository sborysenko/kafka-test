package test.kafka.partitioning

import java.util
import java.util.Properties

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

/**
  * Created by sergey on 5/24/17.
  */
class Consumer(name: String, topic: String) extends Runnable {
  var consumer: KafkaConsumer[String, String] = _
  val props = new Properties()

  def init(): Consumer = {
    println("Read initial state for consumer " + name)

    consumer = getConsumer()
    consumer.subscribe(util.Collections.singletonList("test"))
    consumer.seekToBeginning(consumer.assignment())

    _read("I")

    consumer.unsubscribe()

    this
  }

  def getConsumer(): KafkaConsumer[String, String] ={
    props.put("bootstrap.servers", "localhost:9092")
    props.put("group.id", "test-group")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("enable.auto.commit", "false")
    props.put("auto.commit.interval.ms", "1000")
    props.put("session.timeout.ms", "30000")

    new KafkaConsumer[String, String](props)
  }

  def run(): Unit = {
    println("Starting consumer " + name)

//    consumer = getConsumer()
//    consumer.subscribe(util.Collections.singletonList("test"));

    try {
      while (true) {
        _read("L")
      }
    } finally {
      consumer.close()
    }
  }

  def _read(mark: String): Unit = {
    val inputRecords = consumer.poll(1000)
    val iterator = inputRecords.iterator()
    for (record <- inputRecords) {
      println(mark + "consumer " + name + ": " + record.partition() + ": " + record.offset() + ": " + record.value())
    }
  }
}
