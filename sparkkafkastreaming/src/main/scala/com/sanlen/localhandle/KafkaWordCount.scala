package com.streaming.localhandle

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 实时读取KafKa中的数据  success
  * SparkStream消费端
  * 目前StreamSpark从Kafka拉取数据的方式有两种，
  *  ①一种是基于Receiver的，
  *  ②一种是直接访问不依赖于Receiver的createDirectStream
  * @ author: LuHongGang
  * @ time:   2017/11/13
  * @ version: 1.0
  * 在yarn上运行 bin/spark-submit --class com.streaming.localhandle.KafkaWordCount --master yarn /root/kafka-streaming.jar
  */
object KafkaWordCount {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")//.setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(5))

    val topicMap = Map("test" -> 1)

    val lines = KafkaUtils.createStream(ssc, "Hive-EL67-0001:2181", "testWordCountGroup", topicMap).map(_._2)

    val words = lines.flatMap(_.split(" "))
    val counts = words.map((_, 1L)).reduceByKey(_ + _)

    counts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
