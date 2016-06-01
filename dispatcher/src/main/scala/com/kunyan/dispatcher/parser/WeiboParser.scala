package com.kunyan.dispatcher.parser

import org.jsoup.Jsoup

import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSON

/**
  * Created by niujiaojiao on 2016/5/24.
  */
object WeiboParser {

  /**
    * 解析微博网
    *
    * @param html 将要解析的文本字符串
    * @return 新闻标题的url链接
    */
  def parse(html: String): ListBuffer[String] = {

    var info = ListBuffer[String]()

    val doc = Jsoup.parse(html, "UTF-8")
    val index = doc.getElementsByTag("script").size - 1
    val docStr = doc.getElementsByTag("script").get(index).toString
    val result = docStr.substring(16, docStr.length - 10)
    val jsonInfo = JSON.parseFull(result)

    try {

      if (jsonInfo.isEmpty) {
        println("\"JSON parse value is empty,please have a check!\"")
      } else {

        jsonInfo match {

          case Some(mapInfo: Map[String, AnyVal]) => {

            val newHtml = mapInfo.get("html").get.toString
            val newDoc = Jsoup.parse(newHtml, "UTF-8")
            val children = newDoc.getElementsByAttribute("tbinfo")
            for (i <- 0 until children.size) {

              val child = children.get(i).toString.split("<!")(0).trim()
              val ouidStr = child.split("class")(0).split("ouid=")(1)
              val ouid = ouidStr.substring(0, ouidStr.length - 3)
              val midStr = child.split("action-type")(0).split("mid=")(1)
              val mid = midStr.substring(2, midStr.length - 4)
              info += mid + "||" + ouid
            }
          }
          case None => println("Parsing failed!")
          case other => println("Unknown data structure :" + other)

        }

      }

      info
    } catch {
      case e: Exception =>
        e.printStackTrace()
        null
    }

  }


}
