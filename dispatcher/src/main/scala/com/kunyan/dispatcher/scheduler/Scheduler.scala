package com.kunyan.dispatcher.scheduler


import java.util.Date

import _root_.kafka.serializer.StringDecoder
import com.kunyan.dispatcher.config.{LazyConnections, Platform}
import com.kunyan.dispatcher.parser.{BaiduParser, TaogubaParser}
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.parsing.json.JSON

/**
  * Created by yangshuai on 2016/4/26.
  * 机器人项目中消息处理的主流程类
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
          val attrId = messageMap.get("attr_id").get

          val dbResult = getHtml(tableName, rowkey, lazyConnBr.value)
          val originUrl = dbResult._1
          val html = dbResult._2

          if (attrId.toInt == Platform.Tieba.id) {
            parseTieba(lazyConnBr, originUrl, html)
          } else if (attrId.toInt == Platform.Taoguba.id) {
            parseTaoguba(lazyConnBr, originUrl, html)
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
    * 处理淘股吧消息
    *
    * @param lazyConnBr 连接容器
    * @param originUrl hbase中取出的url
    * @param html hbase中取出的html
    */
  def parseTaoguba(lazyConnBr: Broadcast[LazyConnections], originUrl: String, html: String): Unit = {
    if (originUrl.startsWith("http://www.taoguba.com.cn")) {

      val messages = TaogubaParser.parse(html)
      println(messages)
      messages.foreach {
        x => {

          println(x)

          val id = x._1
          val title = x._2

          println(id)
          println(title)

          if (id.nonEmpty && title.nonEmpty)
            lazyConnBr.value.sendTask("robot_tiebacomment", getCommentJsonString(id, title))

        }
      }

    }
  }

  /**
    * 处理百度贴吧消息
    *
    * @param lazyConnBr 连接容器
    * @param originUrl hbase中取出的url
    * @param html hbase中取出的html
    */
  def parseTieba(lazyConnBr: Broadcast[LazyConnections], originUrl: String, html: String): Unit = {

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

        lazyConnBr.value.sendTask("robot_tiebacomment", getCommentJsonString(originUrl, tuple))
        lazyConnBr.value.sendTask("robot_tiebacomment", getCommentJsonString(originUrl, (tuple._1, tuple._2, tuple._3, "")))

      }
    }
  }

  /**
    * 拼接往robot_stock topic 发的消息的json字符串
    *
    * @param url 消息中的帖子的url
    * @return json格式的消息的字符串
    */
  def getUrlJsonString(url: String): String = {

    val json = "{\"id\":\"\", \"attrid\":\"%d\", \"cookie\":\"\", \"referer\":\"\", \"url\":\"%s\", \"timestamp\":\"%s\"}"

    json.format(Platform.Tieba.id, url, new Date().getTime.toString)
  }

  /**
    * 拼接百度贴吧回帖json
    *
    * @param url   回帖地址
    * @param tuple 回帖所需参数
    * @return json格式的消息的字符串
    */
  def getCommentJsonString(url: String, tuple: (String, String, String, String)): String = {

    val json = "{\"plat_id\":%d, \"preUrl\":\"%s\", \"kw\":\"%s\", \"fid\":\"%s\", \"tbs\":\"%s\", \"repostid\":\"%s\", \"timestamp\":\"%s\"}"

    json.format(Platform.Tieba.id, url, tuple._1, tuple._2, tuple._3, tuple._4, new Date().getTime.toString)
  }

  /**
    * 拼接淘股吧回帖json
    *
    * @param id 帖子id
    * @param title 帖子标题
    * @return json格式的消息的字符串
    */
  def getCommentJsonString(id: String, title: String): String = {

    val json = "{\"plat_id\":%d, \"id\":\"%s\", \"title\":\"%s\", \"timestamp\":\"%s\"}"

    val result = json.format(Platform.Taoguba.id, id, title, new Date().getTime.toString)
    println(result)
    result
  }

  /**
    * 从json字符串中解析数据
    *
    * @param source json字符串
    * @return map形式的json数据
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

  /**
    * 根据表名和rowkey从hbase中获取数据
    *
    * @param tableName 表名
    * @param rowkey 索引
    * @param lazyConn 连接容器
    * @return (url, html)
    */
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
