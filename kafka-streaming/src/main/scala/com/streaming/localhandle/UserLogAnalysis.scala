package com.streaming.localhandle

import com.streaming.utils.ConnectPoolUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.{SparkConf, SparkContext}

/** SparkStreaming读取KafKa中的日志数据并结合SparkSql分析处理 success
  * 统计每一分钟的pv,ip,uv
  * author : LuHongGang
  * time   : 2017/11/16
  * version: 1.0
  */
object UserLogAnalysis {
  private val sparkConf = new SparkConf().setAppName("UserLogAnalysis")
  private val sc = new SparkContext(sparkConf)
  private val ssc = new StreamingContext(sc, Seconds(60))
  case class DapLog(daytime:String, ip:String, cookieid:String)
  def main(args: Array[String]): Unit = {
    //从Kafka中读取数据，topic为daplog，该topic包含两个分区
    val kafkaStream = KafkaUtils.createStream(
      ssc,
      "Hive-EL67-0001:2181",                     //Kafka集群使用的zookeeper
      "group",                                   //该消费者使用的group.id
      Map[String, Int]("test" -> 1),             //日志在Kafka中的topic及其分区
      StorageLevel.MEMORY_AND_DISK_SER)
      .map(x => x._2.split("\\|~\\|", -1))       //日志以|~|为分隔符

    kafkaStream.foreachRDD((rdd: RDD[Array[String]], time: Time) => {
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._
      //构造case class: DapLog,提取日志中相应的字段
      val logDataFrame = rdd.map(w => DapLog(w(0).substring(0, 10),w(2),w(4))).toDF()
      //注册为tempTable
      logDataFrame.registerTempTable("datalog")
      //查询该批次的pv,ip数,uv  current_timestamp()
      val logCountsDataFrame =
      sqlContext.sql("select date_format(current_timestamp(),'yyyy-MM-dd') " +
        "as daytime,count(1) as pv,count(distinct ip) as ip,count(distinct cookieid) as cookieid from datalog")
      // 打印查询结果
      logCountsDataFrame.show()
      val logCountsList = logCountsDataFrame.toDF().collect()
        println(logCountsList.length+"------------->>>时间是 : "+logCountsList(0).getAs[String]("daytime")+" ip地址 : "+logCountsList(0).getAs[Long]("ip")+" uv量 : "+logCountsList(0).getAs[Long]("cookieid"))
      // 处理的结果 入库 (nosql--> hbase/redis 或关系型DB -->Mysql/Oracle)
      loadDataToMysql(logCountsList)
    })
    ssc.start()
    ssc.awaitTermination()
  }

  /** 过程
    * 加载数据到MySql
    * @param logCountsList
    */
  def loadDataToMysql(logCountsList:Array[org.apache.spark.sql.Row]){
    val conn = ConnectPoolUtil.getConnection
    conn.setAutoCommit(false);  //设为手动提交
    val  stmt = conn.createStatement()
    stmt.addBatch("insert into datalog(daytime,ipcount,uvcount) values('" +logCountsList(0).getAs[String]("daytime")+"','"+logCountsList(0).getAs[Long]("ip")+"','"+logCountsList(0).getAs[Long]("cookieid")+"')")
    stmt.executeBatch()
    conn.commit()
    conn.close()
  }
}

object SQLContextSingleton {
  @transient  private var instance: SQLContext = _
  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}

/**  运行的脚本
  * ./bin/spark-submit \
    --class com.streaming.localhandle.UserLogAnalysis \
    --master yarn-cluster \
    --executor-memory 2G \
    --num-executors 6 \
    /root/kafka-streaming.jar
  *
  */