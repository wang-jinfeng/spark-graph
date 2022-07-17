package com.jinfeng.id_mapping

import com.alibaba.fastjson.JSONObject
import org.apache.commons.lang3.StringUtils
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import java.io.UnsupportedEncodingException
import java.security.{MessageDigest, NoSuchAlgorithmException}
import java.text.SimpleDateFormat
import java.util.UUID
import scala.collection.mutable.ArrayBuffer

case class Result(device_id: String, device_type: String, one_id: String, update_date: String) extends Serializable

case class OneIDScore(one_id: String, one_type: String, one_score: Double, one_version: String) extends Serializable

object IDMappingGraphV2 {

  val iosIDSet = Array("idfa", "sysid", "bmosv_osv_upt", "bmosv_upt", "bmosv_ipua", "bkupid")

  val iosMainIDSet = Set("idfa", "sysid")

  val androidCNIDSet = Array("imei", "oaid", "gaid", "sysid", "android_id", "bmosv_upt", "bmosv_ipua", "bkupid")

  val androidIDSet = Array("gaid", "imei", "oaid", "sysid", "android_id", "bmosv_upt", "bmosv_ipua", "bkupid")

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
      .config("spark.rdd.compress", "true")
      .config("spark.sql.orc.filterPushdown", "true")
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
    val df = spark.read.orc("/Users/wangjf/Downloads/part-00000-8d2800cd-b011-4a27-bcf7-85a08d4afcf3-c000.zlib.orc")

    val todayDF = spark.createDataFrame(df.rdd.map(row => {
      processData(row, platform)
    }), schema = schema)

    val schedule_date = sdf1.format(sdf2.parse(date))
    val vertex = todayDF.rdd.map(row => {
      processVertex(schedule_date, row, idSet, idMainSet)
    }).flatMap(l => l)

    val edge = todayDF.rdd.map(row => {
      processEdge(schedule_date, row, idSet, idMainSet)
    }).flatMap(l => l)

    //  vertex.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val graph = Graph(vertex, edge)

    val maxGraph = graph.connectedComponents()

    val result: VertexRDD[VertexId] = maxGraph.vertices

    val resultVertex = result.map(rs => (rs._2, rs._1)).groupByKey()
      .map(rs => {
        val array = new ArrayBuffer[(VertexId, String)]()
        val uuid = StringUtils.replace(UUID.randomUUID().toString, "-", "")
        rs._2.foreach(verId => {
          array += ((verId, uuid))
        })
        array
      }).flatMap(l => l)

    val oneIDResult = resultVertex.join(vertex).mapPartitions(rs => {
      rs.map(r => {
        (r._2._1, r._2._2)
      })
    }).groupByKey().mapPartitions(rs => {
      val array = new ArrayBuffer[Result]()
      val json = new JSONObject()
      rs.foreach(r => {
        r._2.toList.sortBy(t => (idSet.indexOf(t._2), t._1))(Ordering.Tuple2(Ordering.Int, Ordering.String))
          .foreach(one => {
            if (json.isEmpty) {
              json.put("one_id", one._1)
              json.put("type", one._2)
              json.put("version", "")
            }
            array += Result(one._1, one._2, json.toJSONString, "")
          })
      })
      array.iterator
    })

    oneIDResult.take(100).foreach(println)
  }

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

  def processVertex(date: String, row: Row, ids: Array[String], mainIDSet: Set[String]): ArrayBuffer[(Long, (String, String, Long))] = {
    val array = new ArrayBuffer[(Long, (String, String, Long))]()
    //  事件频次
    val cnt = row.getAs[Long]("cnt")
    //  date 活跃日期，用于计算权重
    var mainVertexID = 0L
    for (keyType <- ids if StringUtils.isNotBlank(row.getAs[String](keyType))) {
      val deviceID = if (row.getAs[String](keyType).matches(md5Ptn)) {
        row.getAs[String](keyType)
      } else {
        hashMD5(row.getAs[String](keyType))
      }
      if (mainVertexID == 0) {
        mainVertexID = getMD5Long(deviceID)
      }
      var keyVertexID = getMD5Long(deviceID)
      if (!mainIDSet.contains(keyType)) {
        keyVertexID = keyVertexID + mainVertexID
      }
      array += ((keyVertexID, (deviceID, keyType, cnt)))
    }
    array
  }

  def processEdge(date: String, row: Row, ids: Array[String], mainIDSet: Set[String]): ArrayBuffer[Edge[String]] = {
    val array = new ArrayBuffer[Edge[String]]()
    var flag = true
    for (i <- ids.indices) {
      var keyType = ids(i)
      if (StringUtils.isNotBlank(row.getAs[String](keyType)) && flag) {
        var mainVertexID = if (row.getAs[String](keyType).matches(md5Ptn)) {
          getMD5Long(row.getAs[String](String.valueOf(keyType)))
        } else {
          getMD5Long(hashMD5(row.getAs[String](keyType)))
        }

        /**
         * 判断是否是主节点类型
         * 否 —> 辅节点 VertexID +  主节点 VertexID，避免IPUa等碰撞率高的节点互串，造成数据异常
         * 是 —> 主节点 VertexID
         */
        if (!mainIDSet.contains(keyType)) {
          mainVertexID = mainVertexID + mainVertexID
        }
        for (j <- i + 1 until ids.length) {
          keyType = ids(j)
          if (StringUtils.isNotBlank(row.getAs[String](keyType))) {
            val keyVertexID = if (row.getAs[String](keyType).matches(md5Ptn)) {
              getMD5Long(row.getAs[String](keyType))
            } else {
              getMD5Long(hashMD5(row.getAs[String](keyType)))
            }
            if (!mainIDSet.contains(keyType)) {
              array += Edge(mainVertexID, keyVertexID + mainVertexID, "")
            } else {
              array += Edge(mainVertexID, keyVertexID, "")
            }
          }
        }
        flag = false
      }
    }
    array
  }

  /**
   *
   * @param kv
   * @param mainIDSet
   * @return
   * ((srcID,srcType),oneID)
   */
  def updateOneID(active_date: String, kv: ((String, String), Set[(String, String, Long)]), idArray: Array[String], mainIDSet: Set[String]): ArrayBuffer[((String, String), String)] = {
    val array = new ArrayBuffer[((String, String), String)]()
    val tmpOneId = kv._1._1
    val tmpOneIdType = kv._1._2
    val iters = kv._2
    val oneID = new JSONObject()
    var minTypeIndex = idArray.indexOf(tmpOneIdType)
    iters.foreach(t => {
      if (idArray.indexOf(t._2) < minTypeIndex) {
        minTypeIndex = idArray.indexOf(t._2)
      }
      if (tmpOneId.equals(t._1) || mainIDSet.contains(t._2)) {
        val json = new JSONObject()
        json.put("one_type", t._2)
        json.put("one_date", active_date)
        json.put("one_cnt", t._3)
        oneID.put(t._1, json)
      }
    })
    array += (((tmpOneId, tmpOneIdType), oneID.toJSONString))
    if (idArray.indexOf(tmpOneIdType) > minTypeIndex) {
      iters.map(itr => {
        var oneJSON = new JSONObject()
        if (oneID.containsKey(itr._1)) {
          oneJSON.put(itr._1, oneID.getJSONObject(itr._1))
        } else {
          oneJSON = oneID
        }
        array += (((itr._1, itr._2), oneJSON.toJSONString))
      })
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
}