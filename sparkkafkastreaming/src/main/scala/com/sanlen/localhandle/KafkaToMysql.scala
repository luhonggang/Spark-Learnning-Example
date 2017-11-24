package com.streaming.localhandle

import com.streaming.utils.ConnectPoolUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  *  SparkStreaming 读取kafka中的数据并将数据存储到Mysql中 success
  * @ author: LuHongGang
  * @ time:   2017/11/14
  * @ version: 1.0
  */
object KafkaToMysql {
  def main(args: Array[String]): Unit = {
    // 屏蔽不必要的日志 ,在终端上显示需要的日志
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    Logger.getLogger("org.apache.kafka.clients.consumer").setLevel(Level.OFF)
    val sparkConf = new SparkConf().setAppName("KafkaToMysql") //setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc,Seconds(5))
    //设置连接Kafka的配置信息
    val zkQuorum  = "Hive-EL67-0001:9092,Hive-EL67-0002:9092,Kafka-EL67-0001:9092"    //zookeeper集群的IP：port，IP：port，IP：port
    val group = "testgroup"                  //在consumer.properties配置group.id
    //val topics = "test"                      //选择要连接的producer，它是以topic来区分每个producer的。例如：我这里的创建的topic是test
    //val numThreads = 2                       //线程
    //val topicpMap = topics.split("\n").map((_,numThreads.toInt)).toMap     //这个是有可能有好几个topic同时提供数据，那么我们要把它用空格分割开，然后映射成(topic,2),再转换成map集合
    val topicMap = Map("test" -> 1)
    /** checkpoint容错
      * 设置window上的checkpoint目录
      * 元数据的checkpoint是用来恢复当驱动程序失败的场景下
      * 而数据本身或者RDD的checkpoint通常是用来容错有状态的数据处理失败的场景
      * 启用 checkpoint，需要设置一个支持容错 的、可靠的文件系统（如 HDFS、s3 等）目录来保存 checkpoint 数据。
      * 通过调用 streamingContext.checkpoint(checkpointDirectory) 来完成
      */
   // ssc.checkpoint("C:\\Users\\Administrator\\Desktop\\checkpoint")                                        //topicpMap
    val lines: DStream[String] = KafkaUtils.createStream(ssc,zkQuorum,group,topicMap).map(_._2)    //创建流

    lines.print()

    //保存到mysql
    lines.map(x=>x.split(" ")).foreachRDD(line =>{
      line.foreachPartition(rdd =>{
        val conn = ConnectPoolUtil.getConnection

        conn.setAutoCommit(false);  //设为手动提交
        val  stmt = conn.createStatement()
        rdd.foreach(word=>{
          println("数据1 : "+word(0)+" 数据2 : "+word(1))
          stmt.addBatch("insert into blog(name,count) values('" + word(0)+"','"+word(1)+"')")
        })
        stmt.executeBatch()
        conn.commit()
        conn.close()
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }

}
