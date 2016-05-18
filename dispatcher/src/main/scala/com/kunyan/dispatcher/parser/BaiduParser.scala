package com.kunyan.dispatcher.parser

import org.jsoup.Jsoup
import org.jsoup.nodes.Element
import org.jsoup.select.Elements

import scala.collection.mutable

/**
  * Created by yang on 4/28/16.
  */
object BaiduParser {

  def topBars(html: String): mutable.Set[String] = {

    val set = mutable.Set[String]()

    val doc = Jsoup.parse(html, "UTF-8")
    val tags = doc.getElementById("ba_list").getElementsByTag("a")

    for (i <- 0 until tags.size()) {
      val url = "http://tieba.baidu.com" + tags.get(i).attr("href")
      set.+=(url)
    }

    set
  }

  def getHotPosts(html: String): mutable.Set[String] = {

    val set = mutable.Set[String]()

    val map = mutable.Map[Int, Element]()

    val doc = Jsoup.parse(html, "UTF-8")

    var tags: Elements = null

    try {
      tags = doc.getElementById("thread_list").getElementsByAttributeValue("class", " j_thread_list clearfix")
    } catch {
      case e: NullPointerException =>
        tags = Jsoup.parse(doc.getElementById("pagelet_html_frs-list/pagelet/thread_list").toString.split("<!--")(1).split("-->")(0).trim()).getElementById("thread_list").getElementsByAttributeValue("class", " j_thread_list clearfix")
    }

    for (i <- 0 until tags.size()) {

      val iTag = tags.get(i)

      try {

        val readCount = StringToInt(iTag.getElementsByAttributeValue("class", "row").get(0).getElementsByTag("span").get(1).text)
        val commentCount = StringToInt(iTag.getElementsByAttributeValue("class", "row").get(1).getElementsByTag("span").get(1).text)

        map.put(readCount + commentCount * 2, iTag)
      } catch {
        case e: Exception =>
          val replyCount = StringToInt(iTag.getElementsByAttributeValue("class", "threadlist_rep_num center_text").get(0).text)
          map.put(replyCount, iTag)
      }

    }

    val sortedMap = map.toSeq.sortWith(_._1 > _._1)
    for (i <- 0 until 3) {
      val url = sortedMap(i)._2.getElementsByTag("a").attr("href")
      set.add("http://tieba.baidu.com" + url)
    }

    set
  }

  def getFirstPost(html: String): String = {
    val doc = Jsoup.parse(html, "UTF-8")
    val href = doc.getElementById("content").getElementsByClass("simple_block_container").first().firstElementSibling().firstElementSibling().getElementsByTag("a").attr("href")
    "http://tieba.baidu.com" + href
  }

  def getTitle(html: String): String = {
    Jsoup.parse(html, "UTF-8").title()
  }

  def getUserInfo(html: String): (String, String, String, String) = {

    val doc = Jsoup.parse(html, "UTF-8")
    val title = doc.title
    try {

      var pid = ""
      var barName = title.split("_")(1)
      barName = barName.substring(0, barName.length - 1)
      val fid = html.split("fid: '")(1).split("'")(0)
      val tbs = html.split("tbs:'")(1).split("',")(0)
      val text = doc.select("cc div").get(1).toString

      if (text.nonEmpty) {
        pid = text.split("content_")(1).split("\"")(0)
      }

      (barName, fid, tbs, pid)

    } catch {
      case e: Exception =>
        e.printStackTrace()
        null
    }

  }

  def StringToInt(text: String): Int = {

    if (text.contains("万")) {
      (text.replace("万", "").toFloat * 10000).toInt
    } else {
      text.toInt
    }
  }
}
