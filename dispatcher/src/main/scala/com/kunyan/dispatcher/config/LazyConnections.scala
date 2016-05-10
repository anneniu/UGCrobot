package com.kunyan.dispatcher.config

import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

/**
  * Created by yangshuai on 2016/3/9.
  */
class LazyConnections(createHbaseConnection: () => Connection,
                      createProducer: () => Producer[String, String]) extends Serializable {

  lazy val connection = createHbaseConnection()

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

  def sendTask(topic: String, values: Seq[String]): Unit = {

    val messages = values.map(x => new KeyedMessage[String, String](topic, x))

    try {
      producer.send(messages: _*)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  def getAdmin = connection.getAdmin

  def getTable(tableName: String) = connection.getTable(TableName.valueOf(tableName))

}

object LazyConnections {

  def apply(configFilePath: String): LazyConnections = {

    val createHbaseConnection = () => {

      val hbaseConf = HBaseConfiguration.create
      hbaseConf.set("hbase.rootdir", "hdfs://master:9000/hbase")
      hbaseConf.set("hbase.zookeeper.quorum", "master,slave1,slave2")

      val connection = ConnectionFactory.createConnection(hbaseConf)
      sys.addShutdownHook {
        connection.close()
      }

      connection
    }

    val createProducer = () => {

      val props = new Properties()
      props.put("metadata.broker.list", "master:9092")
      props.put("serializer.class", "kafka.serializer.StringEncoder")
      props.put("producer.type", "async")

      val config = new ProducerConfig(props)
      val producer = new Producer[String, String](config)
      sys.addShutdownHook{
        producer.close()
      }
      producer
    }

    new LazyConnections(createHbaseConnection, createProducer)
  }

}

