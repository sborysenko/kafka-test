package test.kafka.partitioning

import java.util.Properties
import java.util.concurrent.{Executors, ExecutorService}

import org.apache.kafka.common.serialization.StringDeserializer

import scala.Predef.StringFormat

/**
  * Created by sergey on 5/25/17.
  */
object Starter extends App {
  override def main(args: Array[String]): Unit = {
    var executor: ExecutorService = Executors.newFixedThreadPool(5)

    executor.submit(new Runnable {
      override def run(): Unit = new SimpleConsumer("0").consume
    })
    executor.submit(new Runnable {
      override def run(): Unit = new SimpleConsumer("1").consume
    })
    executor.submit(new Runnable {
      override def run(): Unit = new SimpleConsumer("2").consume
    })

    Thread.sleep(24*60*60*1000)
  }
}

