package com.streaming.counts

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * 实时读取KafKa中的数据  success
  * @ author: LuHongGang
  * @ time:   2017/11/13
  * @ version: 1.0
  */
object KafkaWordCount {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(5))

    val topicMap = Map("test" -> 1)

    val lines = KafkaUtils.createStream(ssc, "www.hadoop.com:2181", "testWordCountGroup", topicMap).map(_._2)

    val words = lines.flatMap(_.split(" "))
    val counts = words.map((_, 1L)).reduceByKey(_ + _)

    counts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
