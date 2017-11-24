package com.streaming.localhandle

import com.alibaba.fastjson.JSON
import kafka.serializer.StringDecoder
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


/** Stream
  * author : LuHongGang
  * time   : 2017/11/15
  * version: 1.0
  */
object UserBehaviorAnalysis {
  case class UserEvents(uid:String,source:String,clickCount:Int,time:String)
  private val sparkConf = new SparkConf().setMaster("local[2]").setAppName("UserBehaviorAnalysis")
  private val sc = new SparkContext(sparkConf)
  private val ssc = new StreamingContext(sc, Seconds(60))
  private val sqlContext = new SQLContext(sc)
  def main(args: Array[String]): Unit = {
    // Kafka configurations
    val topics = Set("user_events")
    val brokers = "www.hadoop.com:9092"
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder")
    // Create a direct stream
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
//    kafkaStream.foreachRDD():Unit{
//
//    }
    // 获取到数据
    val events = kafkaStream.flatMap(line => {
        val jsonObj = JSON.parseObject(line._2)
        // json对象是 : {"uid":"b60c4e79-725f-4606-9a13-9fe4d3dd1a9a","os_type":"None","click_count":4,"event_time":"1510733329024"}
        // val jsonStr = JSONObject.formatted(line._2)
        //println("json对象是 : "+jsonObj)
        Some(jsonObj)
    })

    // 遍历所有的对象 并将其注册成临时表
    events.foreachRDD(line =>{
      line.foreachPartition(rdd =>{
       rdd.foreach(obj =>{
         println("每一个对象是 : "+obj)
         //每一个对象是 : {"uid":"e20c5b19-d8ee-46a3-b4e4-d459a759a227","os_type":"Windows Phone","click_count":0,"event_time":"1510733328422"}
       })
      })
    })
    events.foreachRDD(rdd=>{
      UserEvents("uid","source","clickCount".trim.toInt,"time")
    }
    )

   // events.print()
    //val tempDF = events.map(x=>UserEvents(x(0).trim,x(1),x(2).trim.toInt,x(3)))
    // Compute user click times
    val userClicks = events.map(x => (x.getString("uid"), x.getInteger("click_count"))).reduceByKey(_ + _)
    // 来源的平台
    val source = events.map(x=>(x.getString("os_type"),1L)).reduceByKey(_+_)

//    定义更新状态的方法 values为当前批次的单词次数,state为以往的批次的单词的次数
      val updateFunc=(values:Seq[Int],state:Option[Int])=>{
      val currentCount = values.foldLeft(0)(_+_)
      val previousCount = state.getOrElse(0)
      Some(currentCount+previousCount)
    }
    userClicks.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        partitionOfRecords.foreach(pair => {
          //用户ID78 
          val uid = pair._1
          //点击次数
          val click = pair._2
         // println(" ------>>>用户id : "+uid+" -------->>>点击的次数 : "+click)
        })
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
