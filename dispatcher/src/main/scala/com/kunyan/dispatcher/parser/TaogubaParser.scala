package com.kunyan.dispatcher.parser

import org.jsoup.Jsoup

import scala.collection.mutable.ListBuffer

/**
  * Created by yangshuai on 2016/5/11.
  */
object TaogubaParser {

  /**
    * 淘股吧解析类入口
    *
    * @param html 要解析的页面信息字符串
    * @return 返回排名前五的帖子的id和title信息集合
    */
  def parse(html: String): ListBuffer[(String, String)] = {

    var idTitle = ListBuffer[(String, String)]()
    val doc = Jsoup.parse(html, "UTF-8")
    val map = collection.mutable.Map[String, String]()

    try {
      val list = doc.select("div.p_list01 li.pcdj02")

      for (i <- 0 until list.size) {

        val numText = list.get(i).select("b").text()
        val num = numText.substring(1, numText.length - 1)
        map.put(i.toString, num)

      }

      var sortList = ListBuffer[String]()

      map.toArray.sortBy(_._2) foreach {
        x => {
          sortList = sortList ++ ListBuffer(x._1)
        }
      }

      val result: ListBuffer[String] = sortList.reverse.take(5)

      for (j <- result.indices) {

        val children = list.get(result(j).toInt)
        val title = children.select("a").text()
        val id = children.select("a").attr("href").split("/")(1)

        idTitle += (id -> title)
      }

      idTitle

    } catch {
        case e: Exception =>
          e.printStackTrace()
          null
      }

    }


  }
