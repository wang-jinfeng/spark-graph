package com.jinfeng.id_mapping

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import java.io.UnsupportedEncodingException
import java.security.{MessageDigest, NoSuchAlgorithmException}
import java.text.SimpleDateFormat
import java.util.UUID
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class Result(device_id: String, device_type: String, one_id: String, region: String, update_date: String) extends Serializable

case class OneIDScore(one_id: String, one_type: String, one_score: Double, one_version: String) extends Serializable

object IDMappingGraphV2 {

  val iosIDSet = Array("idfa", "sysid", "idfv")

  val iosMainIDSet = Set("idfa", "sysid", "idfv")

  val androidCNIDSet = Array("imei", "oaid", "gaid", "sysid", "android_id")

  val androidIDSet = Array("gaid", "imei", "oaid", "sysid", "android_id")

  val androidCNMainIDSet = Set("gaid", "imei", "oaid", "sysid", "android_id")

  val androidMainIDSet = Set("gaid", "imei", "oaid", "sysid", "android_id")

  val iosIDScoreMap: Map[String, Double] = Map("idfa" -> 1000, "sysid" -> 1, "bmosv_osv_upt" -> 0.9,
    "idfv" -> 0.8, "bmosv_upt" -> 0.7, "bmosv_ipua" -> 0.6, "bkupid" -> 0.3)

  val androidIDScoreMap: Map[String, Double] = Map("gaid" -> 1000, "imei" -> 1000, "oaid" -> 1000, "sysid" -> 1,
    "android_id" -> 0.8, "bmosv_upt" -> 0.7, "bmosv_ipua" -> 0.6, "bkupid" -> 0.3)

  val iosVertSchema: StructType = StructType(Array(
    StructField("idfa", StringType),
    StructField("sysid", StringType),
    StructField("idfv", StringType),
    StructField("bmosv_osv_upt", StringType),
    StructField("bmosv_upt", StringType),
    StructField("bmosv_ipua", StringType),
    StructField("bkupid", StringType),
    StructField("cnt", LongType)
  ))

  val adrCNVertSchema: StructType = StructType(Array(
    StructField("imei", StringType),
    StructField("oaid", StringType),
    StructField("gaid", StringType),
    StructField("sysid", StringType),
    StructField("android_id", StringType),
    StructField("bmosv_osv_upt", StringType),
    StructField("bmosv_upt", StringType),
    StructField("bmosv_ipua_pkg", StringType),
    StructField("bkupid", StringType),
    StructField("cnt", LongType)
  ))

  val adrVertSchema: StructType = StructType(Array(
    StructField("gaid", StringType),
    StructField("imei", StringType),
    StructField("oaid", StringType),
    StructField("sysid", StringType),
    StructField("android_id", StringType),
    StructField("bmosv_osv_upt", StringType),
    StructField("bmosv_upt", StringType),
    StructField("bmosv_ipua", StringType),
    StructField("bkupid", StringType),
    StructField("cnt", LongType)
  ))

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[8]")
      .appName("IDMapping")
      .getOrCreate()
    oldAndTodayIdMapping("CN", "ios", "20220706", spark, "output", "result_output", 10)
  }

  val sdf1 = new SimpleDateFormat("yyyy-MM-dd")
  val sdf2 = new SimpleDateFormat("yyyyMMdd")

  def oldAndTodayIdMapping(country: String, platform: String, date: String, spark: SparkSession, outPutPath: String,
                           resultOutPutPath: String, coalesce: Int) = {

    var dailySQL = ""
    var schema: StructType = null
    var idSet: Array[String] = null
    var idMainSet: Set[String] = null
    var scoreMap: Map[String, Double] = null
    //  1.今日数据加载
    platform match {
      case "ios" =>
        schema = iosVertSchema
        idSet = iosIDSet
        idMainSet = iosMainIDSet
        scoreMap = iosIDScoreMap
        country match {
          case "CN" =>
          //  dailySQL = Constant.ios_id_mapping_sql_v2.replace("@date", date).replace("@filter_country", s"AND country = '${country}'")
          case _ =>
          //  dailySQL = Constant.ios_id_mapping_sql_v2.replace("@date", date).replace("@filter_country", s"")
        }
      case "android" => {
        scoreMap = androidIDScoreMap
        country match {
          case "CN" =>
            idMainSet = androidCNMainIDSet
            schema = adrCNVertSchema
            idSet = androidCNIDSet
          //  dailySQL = Constant.android_id_mapping_sql_v2.replace("@date", date).replace("@filter_country", s"AND country = '${country}'")
          case _ =>
            idMainSet = androidMainIDSet
            schema = adrVertSchema
            idSet = androidIDSet
          //  dailySQL = Constant.android_id_mapping_sql_v2.replace("@date", date).replace("@filter_country", s"")
        }
      }
      case _ =>
        ""
    }

    //  val df = spark.sql(dailySQL)
    val df = spark.read.orc("/Users/mbj0538/Downloads/data/part-00000-8d2800cd-b011-4a27-bcf7-85a08d4afcf3-c000.zlib.orc")

    val sc = spark.sparkContext
    val bTypeIDMap = sc.broadcast(typeIDMap)

    val todayDF = spark.createDataFrame(df.rdd.map(row => {
      processData(row, platform)
    }), schema = schema)

    todayDF.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val schedule_date = sdf1.format(sdf2.parse(date))

    val oldVertex = spark.emptyDataFrame
      .rdd.map(row => {
      val vertexId = row.getAs[Long]("vertex_id")
      val deviceId = row.getAs[String]("device_id")
      val deviceType = row.getAs[String]("device_type")
      val region = row.getAs[String]("region")
      val cnt = row.getAs[Long]("cnt")
      val update_date = row.getAs[String]("update_date")
      val one_id = row.getAs[String]("one_id")
      (vertexId, (deviceId, deviceType, region, cnt, update_date, one_id))
    })
    //  添加 region 信息
    val todayVertex = todayDF.rdd.map(row => {
      processTodayVertex(schedule_date, row, idSet, idMainSet, bTypeIDMap)
    }).flatMap(l => l)
    //  新旧 vertex 合并
    val vertex = oldVertex.union(todayVertex)
      .groupByKey()
      .map(rs => {
        val vertexId = rs._1
        var deviceId = ""
        var deviceType = ""
        var updateDate = ""
        var oneID = ""
        val regionSet = new mutable.HashSet[String]()
        var cnt = 0L
        rs._2.foreach(r => {
          if (StringUtils.isBlank(deviceId)) {
            deviceId = r._1
            deviceType = r._2
          }
          if (StringUtils.isBlank(oneID)) {
            oneID = r._6
          }
          r._3.split(";").foreach(re => {
            regionSet.add(re)
          })
          cnt = cnt + r._4
        })
        (vertexId, (deviceId, deviceType, regionSet.mkString(";"), cnt, "", oneID))
      }).distinct(coalesce * 4)

    vertex.persist(StorageLevel.MEMORY_AND_DISK_SER)

    println("--- vertex.start ---")
    //  vertex.take(20).foreach(println)
    println("--- vertex.end ---")
    /*
    vertex.map(r => {
      OneIDVertex(r._1, r._2._1, r._2._2, r._2._3, r._2._4, r._2._5, r._2._6)
    })
    */

    val oldEdge = spark.emptyDataFrame
      .rdd.map(row => {
      val srcID = row.getAs[Long]("src_id")
      val dstID = row.getAs[Long]("dst_id")
      val attr = row.getAs[String]("attr")
      OneIDEdge(srcID, dstID, attr)
    })

    val todayEdge = todayDF.rdd.map(row => {
      processTodayEdge(schedule_date, row, idSet, idMainSet, bTypeIDMap)
    }).flatMap(l => l)
      .distinct(coalesce * 4)

    todayDF.unpersist()

    //  新旧 edge 合并
    val edge = oldEdge.union(todayEdge)

    edge.persist(StorageLevel.MEMORY_AND_DISK_SER)

    println("--- edge.start ---")
    //  edge.take(20).foreach(println)
    println("--- edge.end ---")

    val graph = Graph(vertex,
      edge.map(r => {
        Edge(r.src_id, r.dst_id, r.attr)
      }))

    val maxGraph = graph.connectedComponents()

    val result: VertexRDD[VertexId] = maxGraph.vertices

    println("--- result.start ---")
    //  result.take(20).foreach(println)
    println("--- result.end ---")

    val resultVertex = result.map(rs => (rs._2, rs._1)).groupByKey()
      .map(rs => {
        val array = new ArrayBuffer[(VertexId, String)]()
        val uuid = StringUtils.replace(UUID.randomUUID().toString, "-", "")
        rs._2.foreach(verId => {
          array += ((verId, uuid))
        })
        array
      }).flatMap(l => l)

    val oneIDVertexDF = resultVertex.join(vertex).mapPartitions(rs => {
      rs.map(r => {
        (r._2._1, r._2._2)
      })
    }).groupByKey()
      .mapPartitions(rs => {
        val array = new ArrayBuffer[OneIDVertex]()
        rs.foreach(r => {
          var json = new JSONObject()
          val sortedList = r._2.toList.sortBy(t => (t._5, idSet.indexOf(t._2), t._4))(Ordering.Tuple3(Ordering.String, Ordering.Int, Ordering.Long))
          sortedList.foreach(one => {
            if (json.isEmpty) {
              if (one._5.nonEmpty) {
                json = JSON.parseObject(one._5)
              } else {
                json.put("one_id", hashMD5(one._1))
                json.put("type", one._2)
                json.put("version", "")
              }
            }
            array += OneIDVertex(getMD5Long(one._1), one._1, one._2, one._3, one._4, json.toJSONString, "")
            //  array += OneIDVertex(one._1, one._2, json.toJSONString, one._3, "")
          })
        })
        array.iterator
      })

    vertex.unpersist()

    oneIDVertexDF.persist(StorageLevel.MEMORY_AND_DISK_SER)

    import spark.implicits._
    //  结果集保存
    oneIDVertexDF.mapPartitions(rs => {
      rs.map(r => {
        Result(r.device_id, r.device_type, r.one_id, r.region, r.update_date)
      })
    }).toDF.take(20).foreach(println)

    oneIDVertexDF
      .toDF.take(20).foreach(println)

    edge
      .toDF.take(20).foreach(println)
  }

  val typeIDMap: Map[String, String] = Map("idfa" -> "10", "idfv" -> "11", "gaid" -> "20", "imei" -> "21", "oaid" -> "22",
    "andorid_id" -> "23", "sysid" -> "00")

  //  IDFA/GAID
  val didPtn = "^[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}$"
  //  全0
  val allZero = "00000000-0000-0000-0000-000000000000"
  //  IMEI
  val imeiPtn = "^([0-9]{14,17})$"
  //  14~16位连续多位相同字符，非法IMEI过滤
  val imeiPtnAll = """^([0-9])\1{14,16}"""
  //  androidId
  val andriodIdPtn = "^[a-fA-F0-9]{16}$"
  //  连续多位相同字符，非法 androidId 过滤
  val andriodIdAll = "^[a-zA-Z0-9]\1{15}$"
  //  MD5
  val md5Ptn = "^([a-fA-F0-9]{32})$"
  //  连续多位相同字符，非法 IMEI MD5 过滤
  val umd5Ptn = """^([0-9A-Za-z])\1{29,31}"""
  //  OAID
  val oaidPtb = """^[0-9A-Za-z-]{16,64}$"""
  //  连续多位相同字符，非法
  val oaidAll = """^([0-9A-Za-z-])\1{16,64}"""
  //  IP
  val ipPtn = """^(25[0-5]|2[0-4][0-9]|[0-1]?[0-9]?[0-9])(\.(25[0-5]|2[0-4][0-9]|[0-1]?[0-9]?[0-9])){3}$"""
  //  Date
  val datePtn = """^\d{4}-\d{2}-\d{2}"""

  def hashMD5(content: String): String = {
    val md5 = MessageDigest.getInstance("MD5")
    val encoded = md5.digest((content).getBytes)
    encoded.map("%02x".format(_)).mkString
  }

  def processData(row: Row, platform: String): Row = {
    platform match {
      case "ios" =>
        var idfa = row.getAs[String]("idfa")
        idfa = if (StringUtils.isNotBlank(idfa) && (idfa.matches(didPtn) && !idfa.matches(allZero) || idfa.matches(md5Ptn))) {
          idfa
        } else {
          ""
        }
        var idfv = row.getAs[String]("idfv")
        idfv = if (StringUtils.isNotBlank(idfv) && (idfv.matches(didPtn) && !idfv.matches(allZero) || idfv.matches(md5Ptn))) {
          idfv
        } else {
          ""
        }
        var sysid = row.getAs[String]("sysid")
        sysid = if (StringUtils.isNotBlank(sysid) && (sysid.matches(didPtn) && !sysid.matches(allZero) || sysid.matches(md5Ptn))) {
          sysid
        } else {
          ""
        }
        var bkupid = row.getAs[String]("bkupid")
        bkupid = if (StringUtils.isNotBlank(bkupid) && (bkupid.matches(didPtn) && !bkupid.matches(allZero) || bkupid.matches(md5Ptn))) {
          bkupid
        } else {
          ""
        }
        val ip = row.getAs[String]("ip")
        val ua = row.getAs[String]("ua")
        val brand = row.getAs[String]("brand")
        val model = row.getAs[String]("model")
        val os_version = row.getAs[String]("os_version")
        val osv_upt = row.getAs[String]("osv_upt")
        val upt = row.getAs[String]("upt")
        val cnt = row.getAs[Long]("cnt")
        val bmosv_osv_upt = if (StringUtils.isNotBlank(osv_upt)) {
          hashMD5(brand + model + os_version + osv_upt)
        } else {
          ""
        }
        val bmosv_upt = if (StringUtils.isNotBlank(upt)) {
          hashMD5(brand + model + os_version + upt)
        } else {
          ""
        }
        val bmosv_ipua = if (StringUtils.isNotBlank(ip)) {
          hashMD5(brand + model + os_version + ip + ua)
        } else {
          ""
        }
        //  IosVert(idfa, sysid, bmosv_osv_upt, bmosv_upt, bmosv_ipua, bkupid, cnt)
        Row(idfa, sysid, idfv, bmosv_osv_upt, bmosv_upt, bmosv_ipua, bkupid, cnt)
      case "android" =>
        var imei = row.getAs[String]("imei")
        imei = if (StringUtils.isNotBlank(imei) && (imei.matches(imeiPtn) && !imei.matches(imeiPtnAll) || imei.matches(md5Ptn))) {
          imei
        } else {
          ""
        }
        var android_id = row.getAs[String]("android_id")
        android_id = if (StringUtils.isNotBlank(android_id) && (android_id.matches(andriodIdPtn) && !android_id.matches(andriodIdAll)
          || android_id.matches(md5Ptn))) {
          android_id
        } else {
          ""
        }
        var oaid = row.getAs[String]("oaid")
        oaid = if (StringUtils.isNotBlank(oaid) && (oaid.length >= 16 && oaid.length <= 64 && !oaid.matches(allZero) || oaid.matches(md5Ptn))) {
          oaid
        } else {
          ""
        }
        var gaid = row.getAs[String]("gaid")
        gaid = if (StringUtils.isNotBlank(gaid) && (gaid.matches(didPtn) && !gaid.matches(allZero) || gaid.matches(md5Ptn))) {
          gaid
        } else {
          ""
        }
        var sysid = row.getAs[String]("sysid")
        sysid = if (StringUtils.isNotBlank(sysid) && (sysid.matches(didPtn) && !sysid.matches(allZero) || sysid.matches(md5Ptn))) {
          sysid
        } else {
          ""
        }
        var bkupid = row.getAs[String]("bkupid")
        bkupid = if (StringUtils.isNotBlank(bkupid) && (bkupid.matches(didPtn) && !bkupid.matches(allZero) || bkupid.matches(md5Ptn))) {
          bkupid
        } else {
          ""
        }
        val country = row.getAs[String]("country")
        val ip = row.getAs[String]("ip")
        val ua = row.getAs[String]("ua")
        val brand = row.getAs[String]("brand")
        val model = row.getAs[String]("model")
        val os_version = row.getAs[String]("os_version")
        val osv_upt = row.getAs[String]("osv_upt")
        val upt = row.getAs[String]("upt")
        val cnt = row.getAs[Long]("cnt")
        val bmosv_osv_upt = if (StringUtils.isNotBlank(osv_upt)) {
          hashMD5(brand + model + os_version + osv_upt)
        } else {
          ""
        }
        val bmosv_upt = if (StringUtils.isNotBlank(upt)) {
          hashMD5(brand + model + os_version + upt)
        } else {
          ""
        }
        val bmosv_ipua = if (StringUtils.isNotBlank(ip)) {
          hashMD5(brand + model + os_version + ip + ua)
        } else {
          ""
        }
        //  AdrVert(imei, gaid, oaid, sysid, android_pkg, bmosv_upt, bmosv_ipua_pkg, xwho, user_id, bkupid, cnt)
        if ("CN".equalsIgnoreCase(country)) {
          Row(imei, oaid, gaid, sysid, android_id, bmosv_osv_upt, bmosv_upt, bmosv_ipua, bkupid, cnt)
        } else {
          Row(gaid, imei, oaid, sysid, android_id, bmosv_osv_upt, bmosv_upt, bmosv_ipua, bkupid, cnt)
        }
      case _ =>
        Row("")
    }
  }

  def processTodayVertex(scheduleDate: String, row: Row, ids: Array[String], mainIDSet: Set[String],
                         bTypeIDMap: Broadcast[Map[String, String]]): ArrayBuffer[(Long, (String, String, String, Long, String, String))] = {
    val array = new ArrayBuffer[(Long, (String, String, String, Long, String, String))]()
    //  事件频次
    val cnt = row.getAs[Long]("cnt")
    //  val region = row.getAs[String]("region")
    val region = ""
    //  date 活跃日期，用于计算权重
    var mainVertexID = 0L
    for (keyType <- ids if StringUtils.isNotBlank(row.getAs[String](keyType))) {
      val typeID = bTypeIDMap.value(keyType)
      val deviceID = if (row.getAs[String](keyType).matches(md5Ptn)) {
        row.getAs[String](keyType) + typeID
      } else {
        hashMD5(row.getAs[String](keyType)) + typeID
      }
      if (mainVertexID == 0) {
        mainVertexID = getMD5Long(deviceID)
      }
      var keyVertexID = getMD5Long(deviceID)
      if (!mainIDSet.contains(keyType)) {
        keyVertexID = keyVertexID + mainVertexID
      }
      array += ((keyVertexID, (deviceID, typeID, region, cnt, new JSONObject().toJSONString, scheduleDate)))
    }
    array
  }

  def processTodayEdge(date: String, row: Row, ids: Array[String], mainIDSet: Set[String],
                       bTypeIDMap: Broadcast[Map[String, String]]): ArrayBuffer[OneIDEdge] = {
    val array = new ArrayBuffer[OneIDEdge]()
    var flag = true
    for (i <- ids.indices) {
      var keyType = ids(i)
      if (StringUtils.isNotBlank(row.getAs[String](keyType)) && flag) {
        var typeID = bTypeIDMap.value(keyType)
        val mainVertexID = if (row.getAs[String](keyType).matches(md5Ptn)) {
          getMD5Long(row.getAs[String](String.valueOf(keyType)) + typeID)
        } else {
          getMD5Long(hashMD5(row.getAs[String](keyType)) + typeID)
        }

        /**
         * 判断是否是主节点类型
         * 否 —> 辅节点 VertexID +  主节点 VertexID，避免IPUa等碰撞率高的节点互串，造成数据异常
         * 是 —> 主节点 VertexID
         */
        val mainKeyVertexID = if (!mainIDSet.contains(keyType)) {
          mainVertexID + mainVertexID
        } else {
          mainVertexID
        }
        for (j <- i + 1 until ids.length) {
          keyType = ids(j)
          if (StringUtils.isNotBlank(row.getAs[String](keyType))) {
            typeID = bTypeIDMap.value(keyType)
            val keyVertexID = if (row.getAs[String](keyType).matches(md5Ptn)) {
              getMD5Long(row.getAs[String](keyType) + typeID)
            } else {
              getMD5Long(hashMD5(row.getAs[String](keyType)) + typeID)
            }
            if (!mainIDSet.contains(keyType)) {
              array += OneIDEdge(mainKeyVertexID, keyVertexID + mainVertexID, "")
            } else {
              array += OneIDEdge(mainKeyVertexID, keyVertexID, "")
            }
          }
        }
        flag = false
      }
    }
    array
  }

  def getMD5Long(str: String): Long = {
    val id_map: Map[String, String] = Map("a" -> "1", "b" -> "2", "c" -> "3", "d" -> "4", "e" -> "5", "f" -> "6")
    var strm = ""
    try {
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

  case class OneIDVertex(vertex_id: Long, device_id: String, device_type: String, region: String, cnt: Long,
                         one_id: String, update_date: String) extends Serializable

  case class OneIDEdge(src_id: Long, dst_id: Long, attr: String) extends Serializable
}