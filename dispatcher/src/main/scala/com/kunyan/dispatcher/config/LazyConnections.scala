package com.kunyan.dispatcher.config

import java.util
import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

/**
  * Created by yangshuai on 2016/3/9.
  */
class LazyConnections(createProducer: () => Producer[String, String]) extends Serializable {

  lazy val producer = createProducer()

  def sendTask(topic: String, value: String): Unit = {

    val message = new KeyedMessage[String, String](topic, value)

    try {
      producer.send(message)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

}

object LazyConnections {

  def apply(configFilePath: String): LazyConnections = {

    val createProducer = () => {

      val props = new Properties()
      props.put("metadata.broker.list", "222.73.57.12:9092")
      props.put("serializer.class", "kafka.serializer.StringEncoder")
      props.put("producer.type", "async")

      val config = new ProducerConfig(props)
      val producer = new Producer[String, String](config)
      sys.addShutdownHook{
        producer.close()
      }
      producer
    }

    new LazyConnections(createProducer)
  }

}

