package com.streaming.localhandle

import com.alibaba.fastjson.JSON
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author : LuHongGang  success
  * time   : 2017/11/16
  * version: 1.0
  */
object UserClickCounts {
  private val sparkConf = new SparkConf().setMaster("local[2]").setAppName("UserClickCounts")
  private val sc = new SparkContext(sparkConf)
  private val ssc = new StreamingContext(sc, Seconds(60))
  def main(args: Array[String]): Unit = {
    // Kafka configurations
    val topics = Set("user_events")
    val brokers = "www.hadoop.com:9092"
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder")
    // Create a direct stream
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    // 获取到数据
    val events = kafkaStream.flatMap(line => {
      val jsonObj = JSON.parseObject(line._2)
      // json对象是 : {"uid":"b60c4e79-725f-4606-9a13-9fe4d3dd1a9a","os_type":"None","click_count":4,"event_time":"1510733329024"}
      // println("json对象是 : "+jsonObj)
      Some(jsonObj)
    })

    events.print()
    // Compute user click times
    val userClicks = events.map(x => (x.getString("uid"), x.getInteger("click_count"))).reduceByKey(_ + _)
    userClicks.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        partitionOfRecords.foreach(pair => {
          //用户ID78
          val uid = pair._1
          //点击次数
          val click = pair._2
          println(" ------>>>用户id : "+uid+" -------->>>点击的次数 : "+click)
        })
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }

}
