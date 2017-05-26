package test.kafka.partitioning

import java.util.Properties
import java.util.concurrent.{Executors, ExecutorService}

import org.apache.kafka.common.serialization.StringDeserializer

import scala.Predef.StringFormat

/**
  * Created by sergey on 5/25/17.
  */
object Starter {
  def main(args: Array[String]): Unit = {
    var executor: ExecutorService = Executors.newFixedThreadPool(2)
    executor.submit(new Runnable {
      override def run(): Unit = {
        new BloombergConsumer("0").process()
      }
    })
    executor.submit(new Runnable {
      override def run(): Unit = {
        new BloombergConsumer("1").process()
      }
    })

    Thread.sleep(24*60*60*1000)
  }
}

