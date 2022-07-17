package com.jinfeng.id_mapping

import com.alibaba.fastjson.{JSON, JSONObject}
import com.jinfeng.utils.DateUtils
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

import java.net.URI
import java.security.MessageDigest
import java.text.SimpleDateFormat
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

object IDMappingGraph {

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
      .master("local")
      .appName("IDMapping")
      .config("spark.rdd.compress", "true")
      .config("spark.sql.orc.filterPushdown", "true")
      .getOrCreate()

    /*
    val sc = spark.sparkContext
    val rdd = sc.parallelize(Seq(("key1", ("A", "1")), ("key1", ("B", "1")), ("key1", ("C", "1")), ("key1", ("D", "1")),
      ("key2", ("a", "1")), ("key2", ("b", "1")), ("key3", ("d", "1"))))

    val map = rdd.countByKey()
    rdd.distinct().collectAsMap().foreach(ks => {
      println("count : " + map(ks._1))
    })
    rdd.distinct().combineByKey(
      (v: (String, String)) => Iterable(v),
      (c: Iterable[(String, String)], v: (String, String)) => c ++ Seq(v),
      (c1: Iterable[(String, String)], c2: Iterable[(String, String)]) => c1 ++ c2
    ).foreach(println)

    sc.stop()
    */

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

    println(s"schema --->>> ${df.schema}")
    val todayDF = spark.createDataFrame(df.rdd.map(row => {
      processData(row, platform)
    }), schema = schema)

    val schedule_date = sdf1.format(sdf2.parse(date))
    val vertex = todayDF.rdd.map(row => {
      processVertex(schedule_date, row, idSet, idMainSet)
    }).flatMap(l => l)

    println(s"vertex --- start ----")
    vertex.take(100)
      .foreach(println)
    println(s"vertex --- end ----")

    System.exit(-1)
    vertex.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val maxGraphFilter = vertex.map(l => {
      (l._1, 1)
    }).groupByKey().map(l => {
      (l._1, l._2.size)
    }).filter(l => {
      l._2 > 1000
    })
    maxGraphFilter.cache()

    val maxGraph = vertex.leftOuterJoin(maxGraphFilter)
      .mapPartitions(kvs => {
        kvs.map(kv => {
          val key = kv._1
          val value = kv._2
          if (value._2.isEmpty) {
            (key, value._1)
          } else {
            null
          }
        })
      }).filter(line => {
      line != null
    }).combineByKey(
      (v: (String, String, Long)) => Iterable(v),
      (c: Iterable[(String, String, Long)], v: (String, String, Long)) => c ++ Seq(v),
      (c1: Iterable[(String, String, Long)], c2: Iterable[(String, String, Long)]) => c1 ++ c2
    )

    //  非主ID生成OneID
    val multiOneIDRDD = maxGraph.filter(kv => {
      kv._2.size > 1
    }).mapPartitions(rs => new CustomInterator(schedule_date, rs, idSet, idMainSet))
      .flatMap(l => l)

    //  主ID生成OneID
    val singleOneIDRDD = maxGraph.filter(kv => {
      kv._2.size == 1
    }).map(kv => {
      val oneID = new JSONObject()
      kv._2.foreach(t => {
        val json = new JSONObject()
        json.put("one_type", t._2)
        json.put("one_date", schedule_date)
        json.put("one_cnt", t._3)
        oneID.put(t._1, json)
      })
      (kv._1, (oneID.toJSONString, schedule_date))
    })

    val yesDate = DateUtils.getDayByString(date, "yyyyMMdd", -1)

    val updateDate = sdf1.format(sdf2.parse(DateUtils.getDayByString(date, "yyyyMMdd", -7)))

    val oldMidMergeOneIDRDD = spark.sql(
      s"""
         |SELECT device_id, device_type, one_id, update_date
         |  FROM ads.ads_device_id_mapping WHERE dt = '$yesDate' AND source = '${country.toLowerCase}' AND platform = '$platform' AND `type` = 'mid'
         |""".stripMargin)
      .rdd
      .map(row => {
        ((row.getAs[String]("device_id"), row.getAs[String]("device_type")), (row.getAs[String]("one_id"), row.getAs[String]("update_date")))
      }).filter(rs => {
      filterAction(rs._1._2, idMainSet) || (!filterAction(rs._1._2, idMainSet) && rs._2._2.compareTo(updateDate) >= 0)
    })

    val midMergeOneIDRDD = spark.sparkContext.union(Seq(singleOneIDRDD, multiOneIDRDD, oldMidMergeOneIDRDD))
      .combineByKey(
        (v: (String, String)) => Iterable(v),
        (c: Iterable[(String, String)], v: (String, String)) => c ++ Seq(v),
        (c1: Iterable[(String, String)], c2: Iterable[(String, String)]) => c1 ++ c2
      ).map(kv => {
      val srcId = kv._1._1
      val srcType = kv._1._2
      var update_date = ""
      val oneIDJSON = new JSONObject()
      kv._2.foreach(ou => {
        val json = JSON.parseObject(ou._1)
        val keys = json.keySet().asScala
        keys.foreach(key => {
          if (oneIDJSON.containsKey(key) && oneIDJSON.getJSONObject(key).getString("one_date")
            .compareTo(json.getJSONObject(key).getString("one_date")) < 0
            || !oneIDJSON.containsKey(key)) {
            oneIDJSON.put(key, json.getJSONObject(key))
          }
        })
        if (StringUtils.isBlank(update_date) || update_date.compareTo(ou._2) < 0) {
          update_date = ou._2
        }
      })
      Result(srcId, srcType, oneIDJSON.toJSONString, update_date)
    })

    import spark.implicits._
    FileSystem.get(new URI(s"s3://mob-emr-test"), spark.sparkContext.hadoopConfiguration).delete(new Path(outPutPath), true)

    midMergeOneIDRDD.toDF
      .repartition(coalesce)
      .write
      .mode(SaveMode.Overwrite)
      .option("orc.compress", "zlib")
      .orc(outPutPath)

    spark.sql(
      s"""
         |ALTER TABLE ads.ads_device_id_mapping ADD IF NOT EXISTS PARTITION (dt='$date',source='${country.toLowerCase}',platform='$platform',`type`='mid')
         | LOCATION '$outPutPath'
         |""".stripMargin)

    val resultOneID = midMergeOneIDRDD.mapPartitions(rs => {
      rs.map(r => {
        val device_id = r.device_id
        val device_type = r.device_type
        val one_id = JSON.parseObject(r.one_id)
        val update_date = r.update_date
        val keys = one_id.keySet().asScala
        var oneIDScore: OneIDScore = OneIDScore("", "", 0, "")
        keys.foreach(key => {
          val sdf = new SimpleDateFormat("yyyy-MM-dd")
          val json = one_id.getJSONObject(key)
          val id_type = json.getString("one_type")
          val id_type_score = scoreMap(id_type)
          val active_date = json.getString("one_date")
          val cnt = json.getLongValue("one_cnt")
          val days = (sdf.parse(schedule_date).getTime - sdf.parse(active_date).getTime) / 1000 / 3600 / 24 + 1
          val score = id_type_score * 30 / days + 0.1 * cnt
          if (idSet.indexOf(id_type) < idSet.indexOf(oneIDScore.one_type) || idSet.indexOf(oneIDScore.one_type) == -1
            || (idSet.indexOf(id_type) == idSet.indexOf(oneIDScore.one_type) && score >= oneIDScore.one_score)) {
            oneIDScore = OneIDScore(key, id_type, score, active_date)
          }
        })
        val json = new JSONObject()
        json.put("one_id", oneIDScore.one_id)
        json.put("type", oneIDScore.one_type)
        json.put("score", oneIDScore.one_score)
        json.put("version", oneIDScore.one_version)
        Result(device_id, device_type, json.toJSONString, update_date)
      })
    })
  }

  def filterAction(device_type: String, mainIDSet: Set[String]): Boolean = {
    mainIDSet.contains(device_type)
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
        //  剔除 xwho、user_id
        //  val xwho = row.getAs[String]("xwho")
        //  val user_id = row.getAs[String]("user_id")
        //  val country = row.getAs[String]("country")
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

  /**
   *
   * @param date
   * @param row
   * @param ids
   * @param mainIDSet
   * @return
   * (srcID,oneID,oneIDJSON,srcType)
   * (oneID,srcID,oneIDJSON,srcType)
   */
  def processVertex(date: String, row: Row, ids: Array[String], mainIDSet: Set[String]): ArrayBuffer[((String, String), (String, String, Long))] = {
    val array = new ArrayBuffer[((String, String), (String, String, Long))]()
    //  val json = JSON.parseObject(Serialization.write(row))
    //  事件频次
    val cnt = row.getAs[Long]("cnt")
    //  date 活跃日期，用于计算权重
    var flag = true
    for (i <- ids.indices) {
      if (StringUtils.isNotBlank(row.getAs[String](String.valueOf(ids(i)))) && flag) {
        val oneIDType = ids(i)
        val oneID = if (row.getAs[String](String.valueOf(ids(i))).matches(md5Ptn)) {
          row.getAs[String](String.valueOf(ids(i)))
        } else {
          hashMD5(row.getAs[String](String.valueOf(ids(i))))
        }
        array += (((oneID, oneIDType), (oneID, oneIDType, cnt)))
        for (j <- i + 1 until ids.length) {
          if (StringUtils.isNotBlank(row.getAs[String](String.valueOf(ids(j))))) {
            val srcType = ids(j)
            val srcOrg = if (row.getAs[String](srcType).matches(md5Ptn)) {
              row.getAs[String](srcType)
            } else {
              hashMD5(row.getAs[String](srcType))
            }
            if (mainIDSet.contains(oneIDType)) {
              array += (((srcOrg, srcType), (oneID, oneIDType, cnt)))
            } else {
              array += (((oneID, oneIDType), (srcOrg, srcType, cnt)))
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
}

class CustomInterator(active_date: String, iter: Iterator[((String, String), Iterable[(String, String, Long)])],
                      idArray: Array[String], mainIDSet: Set[String]) extends Iterator[ArrayBuffer[((String, String), (String, String))]] {
  def hasNext: Boolean = {
    iter.hasNext
  }

  def next: ArrayBuffer[((String, String), (String, String))] = {
    val kv = iter.next
    val array = new ArrayBuffer[((String, String), (String, String))]()
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
        if (oneID.containsKey(t._1) && oneID.getJSONObject(t._1).getLongValue("one_cnt") < t._3) {
          json.put("one_cnt", oneID.getJSONObject(t._1).getLongValue("one_cnt") + t._3)
        } else {
          json.put("one_cnt", t._3)
        }
        oneID.put(t._1, json)
      }
      finalize()
    })
    array += (((tmpOneId, tmpOneIdType), (oneID.toJSONString, active_date)))
    //  if (idArray.indexOf(tmpOneIdType) > minTypeIndex) {
    iters.foreach(itr => {
      var oneJSON = new JSONObject()
      if (oneID.containsKey(itr._1)) {
        oneJSON.put(itr._1, oneID.getJSONObject(itr._1))
      } else {
        oneJSON = oneID
      }
      array += (((itr._1, itr._2), (oneJSON.toJSONString, active_date)))
      finalize()
    })
    //  }
    array
  }
}