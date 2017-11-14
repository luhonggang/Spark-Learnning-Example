package com.streaming.handle

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * @ author: LuHongGang
  * @ time:   2017/11/13
  * @ version: 1.0
  */
object KafkaSparkDemo {

  def main(args: Array[String]) {

    //System.setProperty("hadoop.home.dir", "E:\\Program Files\\hadoop-2.7.0")
    System.setProperty("HADOOP_USER_NAME","hadoop")
    System.setProperty("HADOOP_USER_PASSWORD","hadoop")
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("kafka-spark-demo")
    val scc = new StreamingContext(sparkConf, Duration(5000))
    scc.sparkContext.setLogLevel("ERROR")
    scc.checkpoint(".") // 因为使用到了updateStateByKey,所以必须要设置checkpoint
    val topics = Set("test") //我们需要消费的kafka数据的topic
    val brokers = "www.hadoop.com:9092"
    val kafkaParam = Map[String, String](
           "zookeeper.connect" -> "www.hadoop.com:2181",
           "group.id" -> "test-consumer-group",
      "metadata.broker.list" -> brokers,// kafka的broker list地址
      "serializer.class" -> "kafka.serializer.StringEncoder"
    )

    val stream: InputDStream[(String, String)] = createStream(scc, kafkaParam, topics)

    stream.map(_._2)      // 取出value
      .flatMap(_.split(" ")) // 将字符串使用空格分隔
      .map(r => (r, 1))      // 每个单词映射成一个pair
      .updateStateByKey[Int](updateFunc)  // 用当前batch的数据区更新已有的数据
      .print() // 打印前10个数据
    scc.start() // 真正启动程序
    scc.awaitTermination() //阻塞等待
  }
  val updateFunc = (currentValues: Seq[Int], preValue: Option[Int]) => {
    val curr = currentValues.sum
    val pre = preValue.getOrElse(0)
    Some(curr + pre)
  }
  /**
    * 创建一个从kafka获取数据的流.
    * @param scc           spark streaming上下文
    * @param kafkaParam    kafka相关配置
    * @param topics        需要消费的topic集合
    * @return
    */
  def createStream(scc: StreamingContext, kafkaParam: Map[String, String], topics: Set[String]) = {
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](scc, kafkaParam, topics)
  }
  //    if (args.length < 4) {
  //      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
  //      System.exit(1)
  //    }

  //    var zkQuorum = "Hive-EL67-0001:9092"
  //    var group="group"
  //    val topics="test"
  //    val numThreads="2"
  //StreamingExamples.setStreamingLogLevels()

//  val Array(zkQuorum, group, topics, numThreads) = args
//  val sparkConf = new SparkConf().setAppName("KafkaWordCount")
//  val ssc =  new StreamingContext(sparkConf, Seconds(2))
//  ssc.checkpoint("checkpoint")
//
//  val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap
//  val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
//  val words = lines.flatMap(_.split(" "))
//  val wordCounts = words.map(x => (x, 1L))
//    .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
//  wordCounts.print()
//
//  ssc.start()
//  ssc.awaitTermination()
//}
}
