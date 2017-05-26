package test.kafka.partitioning

import java.util
import java.util.Properties

import scala.collection.JavaConversions._
import org.apache.kafka.clients.consumer.{ConsumerRecords, ConsumerRebalanceListener, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

/**
  * Created by sergey on 5/25/17.
  */
class BloombergConsumer(name: String) {

  var inputThread: Thread = _

  var inputConsumer: KafkaConsumer[String, String] = _


  def process(): Unit = {

    val inputConsumerProps = new Properties()

    inputConsumerProps.put("bootstrap.servers", "localhost:9092")
    inputConsumerProps.put("enable.auto.commit", "false")
    inputConsumerProps.put("auto.commit.interval.ms", "1000")
    inputConsumerProps.put("session.timeout.ms", "30000")
    inputConsumerProps.put("group.id", "test-group")
    inputConsumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    inputConsumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    val stateConsumerProps = inputConsumerProps

    // Let kafka assign the input topic partition.
    val stateConsumer = new KafkaConsumer[String, String](stateConsumerProps)

    val consumerRebalanceListener = new ConsumerRebalanceListener {

      override def onPartitionsRevoked(collection: util.Collection[TopicPartition]): Unit = {
        if (inputThread != null)
          inputThread.interrupt()
      }

        override def onPartitionsAssigned(collection: util.Collection[TopicPartition]): Unit = {


          var stateRecords: ConsumerRecords[String, String] = null

          // Go to beginning for the partitions assigned to this topic.
          stateConsumer.seekToBeginning(collection)
          println("Assigned partitions [" + name + "]: " + collection)

          // Build state.
          // TODO: Implement scala retry
          do {
            println("init state " + name)
            stateRecords = stateConsumer.poll(50)
            for (record <- stateRecords) {
              println("init " + name + ": " + record.partition() + ": " + record.offset() + ": " + record.value())
            }
          }
          while (stateRecords.count() > 0)

          var inputRecords: ConsumerRecords[String, String] = null

          inputConsumer = new KafkaConsumer[String, String](inputConsumerProps)
          inputConsumer.assign(collection.map { c => new TopicPartition("test-1", c.partition())})

          inputThread = new Thread {
            override def run(): Unit = {
              println("Start consuming " + name)
              while (!isInterrupted()) {
                inputRecords = inputConsumer.poll(1000)
                for (record <- inputRecords) {
                  println("consume " + name + ": " + record.partition() + ": " + record.offset() + ": " + record.value())
                }
              }
            }
          }
          inputThread.start()
        }
    }

    stateConsumer.subscribe(util.Collections.singletonList("test-1"), consumerRebalanceListener)
    stateConsumer.poll(0)
  }
}
