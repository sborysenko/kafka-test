package test.kafka

import java.util.{Properties, Random}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object DataProducer extends App {
  override def main(args: Array[String]): Unit = {
    val random: Random = new Random()
    val producerProps = new Properties()

    producerProps.put("bootstrap.servers", "localhost:9092")
    producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](producerProps)
    var count: Int = 0
    while (count < 10) {
      var value = random.nextInt(100)
      producer.send(new ProducerRecord[String, String]("test-1", value.toString, value.toString))
      count = count + 1
    }

    producer.close();
  }
}
