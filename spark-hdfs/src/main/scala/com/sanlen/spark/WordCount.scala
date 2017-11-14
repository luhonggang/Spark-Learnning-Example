package com.sanlen.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ author: LuHongGang
  * @ time : 2017/11/1
  * @ version: 1.0
  */
object WordCount{
  def main(args: Array[String]): Unit = {
    // SparkContext初始化 并启动一个本地化计算
    val conf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(conf)
    // 读取本地的文件统计
    val textFiles = sc.textFile("D:\\helloscala.txt")
      //下划线是占位符，flatMap是对行操作的方法，对读入的数据进行分割
       textFiles.flatMap(_.split(" "))
      //将每一项转换为key-value，数据是key，value是1
      .map((_,1))
      //将具有相同key的项相加合并成一个
      .reduceByKey(_+_)
         //将分布式的RDD返回一个单机的scala array，在这个数组上运用scala的函数操作，并返回结果到驱动程序
      .collect()
         //循环打印
      .foreach(println)



    // args(0) 表示输入的第一个参数
//    val dataFile = args(0)
//    val output = args(1)
//    val sparkConf = new SparkConf().setAppName("WorldCount")
//    val sparkContext = new SparkContext(sparkConf)
//    val lines = sparkContext.textFile(dataFile)
//    val counts = lines.flatMap(_.split(",")).map(s => (s,1)).reduceByKey((a,b) => a+b)
//    counts.saveAsTextFile(output)
//    sparkContext.stop()
  }
}
