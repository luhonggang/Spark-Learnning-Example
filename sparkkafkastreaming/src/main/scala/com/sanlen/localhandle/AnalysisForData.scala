package com.sanlen.localhandle
import java.sql.{Connection, DriverManager, PreparedStatement}

import com.sanlen.utils.ConnectionPools
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * 5s count a data
  */
object AnalysisForData {
  case class Loging(vtime:Long,muid:String,uid:String,ucp:String,category:String,autoSid:Int,dealerId:String,tuanId:String,newsId:String)

  case class Record(vtime:Long,muid:String,uid:String,item:String,types:String)


  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]) {
    val argc = new Array[String](4)
    argc(0) = "Hive-EL67-0001"
    argc(1) = "test"
    argc(2) = "test1"
    argc(3) = "1"
    val Array(zkQuorum, group, topics, numThreads) = argc
    val sparkConf = new SparkConf().setAppName("AnalysisForData").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(10))


    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap

    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(x=>x._2)
     lines.foreachRDD(r=>println(r))
    val sql = "insert into loging_realtime1(vtime,muid,uid,item,category) values (?,?,?,?,?)"

    //1511406129863&US00001&abcdef01&0001&loving&1000&10001&20001&30001
    //case class Loging(vtime:Long,muid:String,uid:String,ucp:String,category:String,autoSid:Int,dealerId:String,tuanId:String,newsId:String)
    val tmpdf = lines.map(_.split("&")).map(x=>Loging(x(0).trim.toLong,x(1),x(2),x(3),x(4),
      x(5).trim.toInt,x(6),x(7),x(8))).filter(x=>(x.muid!=null && !x.muid.equals("null") && !("").equals(x.muid))).map(x=>Record(x.vtime,x.muid,x.uid,getItem(x.category,x.ucp,x.newsId,x.autoSid.toInt,x.dealerId,x.tuanId),getType(x.category,x.ucp,x.newsId,x.autoSid.toInt,x.dealerId,x.tuanId)))
    tmpdf.filter(x=>x.types!=null).foreachRDD{rdd =>
      println("-------------------->>>>>println this data <<<<<---------------------------")
      rdd.foreach(println)
      rdd.foreachPartition(partitionRecords=>{
        val connection = ConnectionPools.getConnection.getOrElse(null)
        if(connection!=null){
          partitionRecords.foreach(record=>process(connection,sql,record))
          ConnectionPools.closeConnection(connection)
        }
      })
    }
    ssc.start()
    ssc.awaitTermination()
  }

  def getItem(category:String,ucp:String,newsId:String,autoSid:Int,dealerId:String,tuanId:String):String = {
    if(category!=null && !category.equals("null")){
      val pattern = "http://www.ihaha.com/\\d{4}-\\d{2}-\\d{2}/\\d{9}.html"
      val matcher = ucp.matches(pattern)
      if(matcher) {
        ucp.substring(33,42)
      }else{
        null
      }
    }else if(autoSid!=0){
      autoSid.toString
    }else if(dealerId!=null && !dealerId.equals("null")){
      dealerId
    }else if(tuanId!=null && !tuanId.equals("null")){
      tuanId
    }else{
      null
    }
  }

  def getType(category:String,ucp:String,newsId:String,autoSid:Int,dealerId:String,tuanId:String):String = {
    if(category!=null && !category.equals("null")){
      val pattern = "100000726;100000730;\\d{9};\\d{9}"
      val matcher = category.matches(pattern)

      val pattern1 = "http://www.chexun.com/\\d{4}-\\d{2}-\\d{2}/\\d{9}.html"
      val matcher1 = ucp.matches(pattern1)

      if(matcher1 && matcher) {
        "nv"
      }else if(newsId!=null && !newsId.equals("null") && matcher1){
        "ns"
      }else if(matcher1){
        "ne"
      }else{
        null
      }
    }else if(autoSid!=0){
      "as"
    }else if(dealerId!=null && !dealerId.equals("null")){
      "di"
    }else if(tuanId!=null && !tuanId.equals("null")){
      "ti"
    }else{
      null
    }
  }

  def process(conn:Connection,sql:String,data:Record): Unit ={
    try{
      val ps : PreparedStatement = conn.prepareStatement(sql)
      ps.setLong(1,data.vtime)
      ps.setString(2,data.muid)
      ps.setString(3,data.uid)
      ps.setString(4,data.item)
      ps.setString(5,data.types)
      ps.executeUpdate()
    }catch{
      case exception:Exception=>
        logger.warn("Error in execution of query"+exception.printStackTrace())
    }
  }

}
