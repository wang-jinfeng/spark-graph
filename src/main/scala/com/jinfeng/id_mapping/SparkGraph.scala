package com.jinfeng.id_mapping

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.sql.SparkSession
import org.json4s.jackson.Serialization

import java.io.UnsupportedEncodingException
import java.math.BigInteger
import java.security.{MessageDigest, NoSuchAlgorithmException}
import java.util.UUID
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class Ios(idfa: String, idfv: String, bkupid: String, region: String) extends Serializable

object SparkGraph {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("SparkGraph")
      .config("spark.rdd.compress", "true")
      .config("spark.io.compression.codec", "snappy")
      .config("spark.sql.orc.filterPushdown", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val sc = spark.sparkContext

    val mainSet = Set("idfa", "idfv")
    val arr = Array("idfa", "idfv", "bkupid")
    try {
      val rdd = sc.parallelize(Seq(Ios("A", "a", "1.1.1.1mob", "cn"), Ios("B", "a", "1.1.1.1emr", "se"),
        Ios("C", "c", "1.1.1.1emr", "vg"), Ios("A", "d", "1.1.1.1uc", "hk"), Ios("A", "a", "1.1.1.1mob", "cn")))
      val vert = rdd.map(r => {
        val region = r.region
        implicit val formats = org.json4s.DefaultFormats
        val json = JSON.parseObject(Serialization.write(r))
        var mainID = 0L
        val array = new ArrayBuffer[(Long, (String, String, String))]()
        for (key <- arr if StringUtils.isNotBlank(json.getString(key))) {
          if (mainID == 0) {
            mainID = getMD5Long(json.getString(key))
          }
          var keyID = getMD5Long(json.getString(key))
          if (!mainSet.contains(key)) {
            keyID = keyID + mainID
          }
          array += ((keyID, (json.getString(key), key, region)))
        }
        array
      }).flatMap(l => l)
        .groupByKey()
        .map(rs => {
          val vertexId = rs._1
          var deviceId = ""
          var deviceType = ""
          val regionSet = new mutable.HashSet[String]()
          rs._2.foreach(r => {
            if (StringUtils.isBlank(deviceId)) {
              deviceId = r._1
              deviceType = r._2
            }
            regionSet.add(r._3)
          })
          (vertexId, (deviceId, deviceType, regionSet.mkString(";")))
        })

      println("--- Vert.start ---")
      vert.foreach(println)
      println("--- Vert.end ---")

      val edges = rdd.map(r => {
        val array = new ArrayBuffer[Edge[(String)]]()
        implicit val formats = org.json4s.DefaultFormats
        val json = JSON.parseObject(Serialization.write(r))
        var flag = true
        for (i <- arr.indices) {
          if (StringUtils.isNotBlank(json.getString(arr(i))) && flag) {
            val srcOrg = json.getString(arr(i))
            var attr = s"${arr(i)}.${arr(i)}"
            val keyId = if (!mainSet.contains(arr(i))) {
              getMD5Long(srcOrg) + getMD5Long(srcOrg)
            } else {
              getMD5Long(srcOrg)
            }
            for (j <- i + 1 until arr.length) {
              attr = s"${arr(i)}.${arr(j)}"
              val dstOrg = json.getString(arr(j))
              if (!mainSet.contains(arr(j))) {
                array += Edge(keyId, getMD5Long(dstOrg) + getMD5Long(srcOrg), attr)
              } else {
                array += Edge(keyId, getMD5Long(dstOrg), attr)
              }
              //  array += Edge(getMD5Long(dstOrg), getMD5Long(srcOrg), attr)
            }
            flag = false
          }
        }
        array
      }).flatMap(l => l)

      val graph = Graph(vert, edges)

      val maxGraph = graph.connectedComponents()

      val res: VertexRDD[VertexId] = maxGraph.vertices

      //  res.foreach(println)
      //  val res = StronglyConnectedComponents.run(maxGraph, Int.MaxValue).vertices
      //  res.foreach(println)

      //  System.exit(-1)
      //  val res: VertexRDD[VertexId] = maxGraph.vertices
      val resultVert = res.map(tp => (tp._2, tp._1))
        .groupByKey()
        .map(rs => {
          val array = new ArrayBuffer[(VertexId, String)]()
          val uuid = StringUtils.replace(UUID.randomUUID().toString, "-", "")
          rs._2.foreach(verId => {
            array += ((verId, uuid))
          })
          array
        }).flatMap(l => l)

      val uidRDD = resultVert.join(vert).map(r => {
        (r._2._1, r._2._2)
      }).groupByKey()
        .map(r => {
          val array = new ArrayBuffer[Result]()
          val json = new JSONObject()
          r._2.toList.sortBy(t => (arr.indexOf(t._2), t._1))(Ordering.Tuple2(Ordering.Int, Ordering.String))
            .foreach(one => {
              if (json.isEmpty) {
                json.put("one_id", one._1)
                json.put("type", one._2)
                json.put("version", "")
              }
              array += Result(one._1, one._2, json.toJSONString, one._3, "")
            })
          array
        })

      println("--- OneID.start ---")
      uidRDD.foreach(println)
      println("--- OneID.end ---")
    }
    /*
    import org.json4s.native.Serialization
    implicit val formats=org.json4s.DefaultFormats
    val ios = Ios("A", "a", "1.1.1.1", "mob")
    println(Serialization.write(ios))
    */
    //  println(getMD5Long("a"))
  }

  def getMD5Long(str: String): Long = {

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