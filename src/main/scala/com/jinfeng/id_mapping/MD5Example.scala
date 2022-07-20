package com.jinfeng.id_mapping

import java.io.UnsupportedEncodingException
import java.math.BigInteger
import java.security.{MessageDigest, NoSuchAlgorithmException}

object MD5Example {

  def main(args: Array[String]): Unit = {
    /*
    val md5 = hashMD5("")
    println(md5)
    val md5Lang = getMD5Long(md5)
    println(md5Lang)
    println(getMD5LongV2(""))
    */

    println(hashMD5("8711c3c7-87c1-496a-8a24-469d0cd71ecf")+" -- bkupid")
    println(hashMD5("80A476C2-4136-406A-8F12-FB2F6E29F06B")+" -- idfa")
  }

  def hashMD5(content: String): String = {
    val md5 = MessageDigest.getInstance("MD5")
    val encoded = md5.digest((content).getBytes)
    encoded.map("%02x".format(_)).mkString
  }

  def getMD5Long(str: String): Long = {
    val id_map: Map[String, String] = Map("a" -> "1", "b" -> "2", "c" -> "3", "d" -> "4", "e" -> "5", "f" -> "6")
    var strm = ""
    try {
      //  val array = str.getBytes("UTF-8")
      //  val bigInt = new BigInteger(1, array).toString(16).substring(8, 24)
      val bigInt = str.substring(8, 24)
      for (c <- bigInt.toCharArray) {
        if (id_map.contains(String.valueOf(c))) strm += id_map(String.valueOf(c))
        else strm += String.valueOf(c)
      }
    } catch {
      case e@(_: NoSuchAlgorithmException | _: UnsupportedEncodingException) =>
        e.printStackTrace()
    }
    strm.toLong
  }

  def getMD5LongV2(str: String): Long = {
    val id_map: Map[String, String] = Map("a" -> "1", "b" -> "2", "c" -> "3", "d" -> "4", "e" -> "5", "f" -> "6", "g" -> "7")
    var strm = ""
    try {
      //  第一步，获取MessageDigest对象，参数为MD5表示这是一个MD5算法
      val md5 = MessageDigest.getInstance("MD5")
      //  第二步跳过，输入源数据，参数类型为byte[]
      //  第三步，计算MD5值
      val array = md5.digest(str.getBytes("UTF-8"))
      //  第四步，结果转换并返回
      val bigInt = new BigInteger(1, array).toString(16).substring(8, 24)
      for (c <- bigInt.toCharArray) {
        if (id_map.contains(String.valueOf(c))) strm += id_map(String.valueOf(c))
        else strm += String.valueOf(c)
      }
    } catch {
      case e@(_: NoSuchAlgorithmException | _: UnsupportedEncodingException) =>
        e.printStackTrace()
    }
    strm.toLong
  }
}
