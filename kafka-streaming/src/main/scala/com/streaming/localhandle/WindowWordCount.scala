package com.streaming.localhandle

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *  窗口函数
  * author : LuHongGang
  * time   : 2017/11/16
  * version: 1.0
  */
object WindowWordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("WindowWordCount")
    val sc = new SparkContext(sparkConf)
    // 创建sparkStreaming
    val ssc = new StreamingContext(sc,Seconds(5))
    // 定义checkpoint目录为当前目录
    //ssc.checkpoint(".")
    val topicMap = Map("test" -> 1)

    val lines = KafkaUtils.createStream(ssc, "www.hadoop.com:2181", "testWordCountGroup", topicMap).map(_._2)

    val words = lines.flatMap(_.split(" "))
    /**
      * 窗口每滑动一次，落在该窗口中的RDD被一起同时处理，生成一个窗口DStream（windowed DStream），
      * 窗口操作需要设置两个参数：
      （1）窗口长度（window length），即窗口的持续时间，下面的窗口长度为20
      （2）滑动间隔（sliding interval），窗口操作执行的时间间隔，下面的滑动间隔为10
      这两个参数必须是原始DStream 批处理间隔（batch interval）的整数倍（当前程序中原始DStream的batch interval为1）
      不是整数倍 会报错:
      Exception in thread "main" java.lang.Exception: The window duration of windowed DStream (18000 ms)
     must be a multiple of the slide duration of parent DStream (5000 ms)
      */
    val counts = words.map(x => (x , 1)).reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds("20".toInt), Seconds("10".toInt))

    counts.print()

    ssc.start()
    ssc.awaitTermination()



  }

}
