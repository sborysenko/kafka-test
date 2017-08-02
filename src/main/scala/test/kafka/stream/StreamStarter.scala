package test.kafka.stream

import java.util.Properties

import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.processor.{TopologyBuilder, WallclockTimestampExtractor}
import org.apache.kafka.streams.state.Stores

object StreamStarter extends App {
  override def main(args: Array[String]): Unit = {
    var builder = new TopologyBuilder()

    builder.addSource("source", new StringDeserializer, new StringDeserializer, "test-1")
    builder.addProcessor("processor", new StreamProcessorSupplier, "source")
    builder.addStateStore(Stores.create("cache").withStringKeys().withStringValues().inMemory().build(), "processor")
    builder.addSink("destination", "test-2", new StringSerializer, new StringSerializer, "processor")

    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id-stream-processor-1")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181")
    props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, classOf[WallclockTimestampExtractor])

    var streamingConfig = new StreamsConfig(props)

    var streaming = new KafkaStreams(builder, streamingConfig);
    streaming.start()

    println("Application started")

    Thread.sleep(24*60*60*1000)
  }
}
