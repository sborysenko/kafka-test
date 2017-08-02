package test.kafka.stream

import java.util.UUID

import org.apache.kafka.streams.processor.{Processor, ProcessorContext, ProcessorSupplier}
import org.apache.kafka.streams.state.KeyValueStore


class StreamProcessor extends Processor[String, String] {
  var context: ProcessorContext = _
  var kvStore: KeyValueStore[String, String] = _

  override def init(ctx: ProcessorContext): Unit = {
    context = ctx
    context.schedule(1000)

    kvStore = ctx.getStateStore("cache").asInstanceOf[KeyValueStore[String, String]]

    println("Init application.\n Store content")
    var iter = kvStore.all()
    while(iter.hasNext) {
      var entry = iter.next()
      println(s"${entry.key} - ${entry.value}")
    }
  }

  override def process(key: String, value: String): Unit = {
    println(s"Process Partition: ${context.partition()} Key : $key, Value : $value")

    var uuid = kvStore.get(value)

    if (uuid == null) {
      uuid = UUID.randomUUID().toString
      kvStore.putIfAbsent(value, uuid)
      println("UUID not found in store. Generate new UUID")
    } else {
      println(s"UUID:$uuid for value:$value found in store")
    }

    context.forward(value, value + " - " + uuid)
  }

  override def punctuate(timestamp: Long): Unit = {
    kvStore.flush()
    context.commit()
  }

  override def close(): Unit = {
    kvStore.close()
  }
}

class StreamProcessorSupplier extends ProcessorSupplier[String, String] {
  override def get(): Processor[String, String] = new StreamProcessor
}
