/*
package com.streaming.localhandle

import com.streaming.utils.ConnectionPool
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * author : LuHongGang
  * time   : 2017/11/16
  * version: 1.0
  */
object OnlineShopTopCounts {
  def main(args: Array[String]){
    /**
      * 第1步：创建Spark的配置对象SparkConf，设置Spark程序的运行时的配置信息，
      * 例如说通过setMaster来设置程序要链接的Spark集群的Master的URL,如果设置
      * 为local，则代表Spark程序在本地运行，特别适合于机器配置条件非常差（例如
      * 只有1G的内存）的初学者       *
      */
    val conf = new SparkConf() //创建SparkConf对象
    conf.setAppName("OnlineTheTop3ItemForEachCategory2DB") //设置应用程序的名称，在程序运行的监控界面可以看到名称
    //    conf.setMaster("spark://Master:7077") //此时，程序在Spark集群
    conf.setMaster("local[6]")
    //设置batchDuration时间间隔来控制Job生成的频率并且创建Spark Streaming执行的入口
    val ssc = new StreamingContext(conf, Seconds(5))

    ssc.checkpoint("/root/Documents/SparkApps/checkpoint")

    // socket 实现
    //val userClickLogsDStream = ssc.socketTextStream("Master", 9999)
    val  userClickLogsDStream = KafkaUtils.createStream(
      ssc,
      "www.hadoop.com:2181",
      "group",
      Map[String, Int]("test" -> 0,"test" -> 1),
      StorageLevel.MEMORY_AND_DISK_SER)


    val formattedUserClickLogsDStream = userClickLogsDStream.map(clickLog =>
      (clickLog.split(" ")(2) + "_" + clickLog.split(" ")(1), 1))

    //    val categoryUserClickLogsDStream = formattedUserClickLogsDStream.reduceByKeyAndWindow((v1:Int, v2: Int) => v1 + v2,
    //      (v1:Int, v2: Int) => v1 - v2, Seconds(60), Seconds(20))

    val categoryUserClickLogsDStream = formattedUserClickLogsDStream.reduceByKeyAndWindow(_+_,
      _-_, Seconds(60), Seconds(20))

    categoryUserClickLogsDStream.foreachRDD { rdd => {
      if (rdd.isEmpty()) {
        println("No data inputted!!!")
      } else {
        val categoryItemRow = rdd.map(reducedItem => {
          .......
          Row(category, item, click_count)
        })

        val structType = StructType(Array(
          StructField("category", StringType, true),
          StructField("item", StringType, true),
          StructField("click_count", IntegerType, true)
        ))

        val sqlContext = new SQLContext(rdd.context)
        val categoryItemDF = sqlContext.createDataFrame(categoryItemRow, structType)

        categoryItemDF.registerTempTable("categoryItemTable")

        val reseltDataFram = sqlContext.sql("SELECT category,item,click_count FROM .......").toDF()


        val resultRowRDD = reseltDataFram.rdd

        resultRowRDD.foreachPartition { partitionOfRecords => {

          if (partitionOfRecords.isEmpty){
            println("This RDD is not null but partition is null")
          } else {
            // ConnectionPool is a static, lazily initialized pool of connections
            val connection = ConnectionPool.getConnection()
            partitionOfRecords.foreach(record => {
              val sql = "insert into categorytop3.........)"
              val stmt = connection.createStatement();
              stmt.executeUpdate(sql);

            })
            ConnectionPool.returnConnection(connection) // return to the pool for future reuse

          }
        }
        }
      }


    }
    }



    /**
      * 在StreamingContext调用start方法的内部其实是会启动JobScheduler的Start方法，进行消息循环，在JobScheduler
      * 的start内部会构造JobGenerator和ReceiverTacker，并且调用JobGenerator和ReceiverTacker的start方法：
      *   1，JobGenerator启动后会不断的根据batchDuration生成一个个的Job
      *   2，ReceiverTracker启动后首先在Spark Cluster中启动Receiver（其实是在Executor中先启动ReceiverSupervisor），在Receiver收到
      *   数据后会通过ReceiverSupervisor存储到Executor并且把数据的Metadata信息发送给Driver中的ReceiverTracker，在ReceiverTracker
      *   内部会通过ReceivedBlockTracker来管理接受到的元数据信息
      * 每个BatchInterval会产生一个具体的Job，其实这里的Job不是Spark Core中所指的Job，它只是基于DStreamGraph而生成的RDD
      * 的DAG而已，从Java角度讲，相当于Runnable接口实例，此时要想运行Job需要提交给JobScheduler，在JobScheduler中通过线程池的方式找到一个
      * 单独的线程来提交Job到集群运行（其实是在线程中基于RDD的Action触发真正的作业的运行），为什么使用线程池呢？
      *   1，作业不断生成，所以为了提升效率，我们需要线程池；这和在Executor中通过线程池执行Task有异曲同工之妙；
      *   2，有可能设置了Job的FAIR公平调度的方式，这个时候也需要多线程的支持；
      *
      */
    ssc.start()
    ssc.awaitTermination()

  }

}
*/
