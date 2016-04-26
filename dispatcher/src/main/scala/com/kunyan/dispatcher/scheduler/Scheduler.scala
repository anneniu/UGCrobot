package com.kunyan.dispatcher.scheduler


import java.util.Date

import _root_.kafka.serializer.StringDecoder
import com.kunyan.dispatcher.config.LazyConnections
import com.kunyan.dispatcher.util.StringUtil
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
  * Created by yangshuai on 2016/4/26.
  */
object Scheduler {

  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf()
      .setAppName("NewsParser")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "2000")
      .setMaster("local")

    val ssc = new StreamingContext(sparkConf, Seconds(1))
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "222.73.34.92:9092",
      "group.id" -> "robot")

    val lazyConn = ssc.sparkContext.broadcast(LazyConnections(""))

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set("kunyan_to_upload_inter_tab_up","robot_mainpageresult"))

    LogManager.getRootLogger.setLevel(Level.WARN)

    messages.foreachRDD(rdd => {
      rdd.foreach(x => {

        val result = StringUtil.parseJsonObject(x._2)
        val arr = result.split("\t")
        val host = arr(3)
        val uri = arr(4)
        val cookie = arr(6)

        if (host.contains("tieba.baidu.com") && uri.contains("/i/")) {
          val unknownId = uri.split("/")(2)
          val jsonString = getJsonString(cookie, host + "/i/" + unknownId + "/thread")
          println(jsonString)
          lazyConn.value.sendTask("robot_mainpage", jsonString)
          println("Successful")

        }
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

  def getJsonString(cookie: String, url: String): String = {
    val json = "{\"id\":\"%s\", \"attrid\":\"7001\", \"cookie\":\"%s\", \"referer\":\"\", \"url\":\"%s\"}"
    json.format(new Date().getTime.toString, cookie, url)
  }
}
