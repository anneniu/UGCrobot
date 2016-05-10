package com.kunyan.dispatcher.scheduler


import java.util.Date

import _root_.kafka.serializer.StringDecoder
import com.kunyan.dispatcher.config.LazyConnections
import com.kunyan.dispatcher.parser.BaiduParser
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.commons.codec.digest.DigestUtils


import scala.util.parsing.json.JSON

/**
  * Created by yangshuai on 2016/4/26.
  * XX项目主流程类
  */
object Scheduler {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("NewsParser")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "2000")
      .setMaster("local")

    val ssc = new StreamingContext(sparkConf, Seconds(1))
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "master:9092",
      "group.id" -> "robot")

    val lazyConnBr = ssc.sparkContext.broadcast(LazyConnections(""))

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set("robot_stockresult"))

    LogManager.getRootLogger.setLevel(Level.WARN)

    messages.foreachRDD(rdd => {

      rdd.foreach(x => {

        println(x._2)

        val messageMap = parseJson(x._2)

        try {
          val tableName = messageMap.get("key_name").get
          val rowkey = messageMap.get("pos_name").get

          val dbResult = getHtml(tableName, rowkey, lazyConnBr.value)
          val originUrl = dbResult._1
          val html = dbResult._2

          if (originUrl.startsWith("http://tieba.baidu.com/f/index/forumpark")) {

            val messages = BaiduParser.topBars(html).map(getUrlJsonString)
            lazyConnBr.value.sendTask("robot_stock", messages.toSeq)

          } else if (originUrl.startsWith("http://tieba.baidu.com/f?kw=")) {

            val messages = BaiduParser.getHotPosts(html).map(getUrlJsonString)
            println(messages)
            lazyConnBr.value.sendTask("robot_stock", messages.toSeq)

          } else if (originUrl.startsWith("http://tieba.baidu.com/p/")) {

            val tuple = BaiduParser.getUserInfo(html)
            if (tuple != null) {
              val jsonStr = getCommentJsonString(originUrl, tuple, "0")
              println(jsonStr)
              lazyConnBr.value.sendTask("robot_tiebacomment", jsonStr)
              lazyConnBr.value.sendTask("robot_tiebacomment", getCommentJsonString(originUrl, tuple, "1"))

            }
          }
        } catch {
          case e: Exception =>
            e.printStackTrace()
        }

      })

    })

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 拼接json字符串
    */
  def getUrlJsonString(url: String): String = {

    val json = "{\"id\":\"\", \"attrid\":\"7001\", \"cookie\":\"\", \"referer\":\"\", \"url\":\"%s\", \"timestamp\":\"%s\"}"

    json.format(url, new Date().getTime.toString)

  }

  /**
    *
    *
    * @param url
    * @param tuple
    * @param floor
    * @return
    */
  def getCommentJsonString(url: String, tuple: (String, String, String), floor: String): String = {

    val json = "{\"preUrl\":\"%s\", \"kw\":\"%s\", \"fid\":\"%s\", \"tbs\":\"%s\", \"floor_num\":\"%s\", \"repostid\":\"\", \"timestamp\":\"%s\"}"
    json.format(url, tuple._1, tuple._2, tuple._3, floor, new Date().getTime.toString)

  }

  /**
    * 拼接json字符串
    */
  def getCommentJsonString(url: String, barName: String, fid: String, tbs: String): String = {

    val json = "{\"pre_url\":\"%s\", \"kw\":\"%s\", \"fid\":\"%s\", \"floor_num\":\"1\", \"tbs\":\"%s\", \"content\":\"\", \"repostid\":\"\", \"timestamp\":\"%s\"}"
    json.format(url, barName, fid, tbs, new Date().getTime.toString)

  }

  /**
    * 从json字符串中解析数据
    */
  def parseJson(source: String): Map[String, String] = {

    try {

      JSON.globalNumberParser = { input: String => input }
      val json: Option[Any] = JSON.parseFull(source)

      if (json.isDefined) {

        val map: Map[String, String] = json.get.asInstanceOf[Map[String, String]]

        map
      } else {
        null
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        null
    }
  }

  def getHtml(tableName: String, rowkey: String, lazyConn: LazyConnections): (String, String) = {

    val table = lazyConn.getTable(tableName)
    val get = new Get(rowkey.getBytes)

    try {
      val result = table.get(get)
      val url = new String(result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("url")))
      val content = new String(result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("content")))
      (url, content)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        null
    }

  }

}
