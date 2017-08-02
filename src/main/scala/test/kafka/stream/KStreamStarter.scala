package test.kafka.stream

import java.util.{Properties, UUID}

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig}
import org.apache.kafka.streams.kstream._

object KStreamStarter extends App {
  override def main(args: Array[String]): Unit = {
    val builder: KStreamBuilder = new KStreamBuilder

    val values: KStream[String, String] = builder.stream(Serdes.String, Serdes.String, "test-1")

    val out: KStream[String, String] = values.map {
      new KeyValueMapper[String, String, KeyValue[String, String]]() {
        override def apply(key: String, value: String): KeyValue[String, String] = {
          println(s"Key:$key, Value:$value")
          new KeyValue[String, String](value, value + " - " + UUID.randomUUID().toString)
        }
      }
    }

    out.to(Serdes.String, Serdes.String, "test-2")

    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id-stream-processor-1")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181")

    var streamingConfig = new StreamsConfig(props)
    val stream: KafkaStreams = new KafkaStreams(builder, streamingConfig)
    stream.start()

    Thread.sleep(24*60*60*1000)
  }

}
