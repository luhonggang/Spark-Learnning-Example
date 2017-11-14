package com.sanlen.mysql

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 本地化 spark sql 分析数据入库(mysql)
  * @ author: LuHongGang
  * @ time: 2017/11/2
  * @ version: 1.0
  */
object SparkSqlToMysql {
  val url = "jdbc:mysql://localhost:3306/localdb"
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress","false")
    val sparkConf = new SparkConf().setAppName("SparkSqlToMysql").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    // 通过并行化 创建RDD
    val personRDD = {
      sc.parallelize(Array("20 tom 5 5000","15 jerry 3 5623","16 kitty 6 8922")).map(_.split(" "))
    }
    personRDD.foreach(println)
    // 指定字段 的类型
    val schema = StructType(
      List(
        StructField("id",IntegerType,true),
        StructField("name",StringType,true),
        StructField("age",IntegerType,true),
        StructField("salary",IntegerType,true)
      )
    )
    //将RDD映射到rowRDD
    val rowRDD = personRDD.map(p => Row(null, p(1).trim, p(2).toInt,p(3).toInt))
    //将schema信息应用到rowRDD上
    val personDataFrame = sqlContext.createDataFrame(rowRDD,schema)
    //创建Properties存储数据库相关属性
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "123456")
    //将数据追加到数据库  SaveMode.Overwrite
    personDataFrame.write.mode(SaveMode.Append).jdbc(url,
      "localdb.spark_sql_person",prop)
    //停止SparkContext
    sc.stop()
  }
}
