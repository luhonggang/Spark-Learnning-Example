package com.sanlen.mysql
import java.util.Properties

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
/**
  * @ author: LuHongGang
  * @ time: 2017/11/2
  * @ version: 1.0
  */
object EmployAnalysis {
  val connectionUrl = "jdbc:mysql://10.0.0.19:3306/localdb"
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("EmployAnalysis")
    val sc = new SparkContext(conf)
    //指定地址创建rdd
    val sqlContext = new SQLContext(sc)
    // 读取liunx上的本地文件 file:///root/spark_data.txt
    val studentsRDD = sc.textFile("hdfs://Hive-EL67-0001:8020/user/spark_data.txt").map(_.split(","))
    //以编程方式动态构造元素据
    val schema = StructType(
      List(
        StructField("provice", StringType, true),
        StructField("commpany", StringType, true),
        StructField("employ", IntegerType, true),
        StructField("industry", StringType, true),
        StructField("unemployment", IntegerType, true)
      )
    )
    //将rdd映射到rowRDD
    //山东,阿里巴巴,2801,自媒体,1220
    val RowRDD = studentsRDD.map(x => Row(x(0).trim,x(1).trim,x(2).toInt,x(3).trim,x(4).toInt))
    //将schema信息映射到rowRDD
    val studentsDF = sqlContext.createDataFrame(RowRDD, schema)
    //创建Properties存储数据库相关属性
    val prop = new Properties()
    prop.put("user", "lhg")
    prop.put("password", "123456")
    // 原始数据保存
    studentsDF.write.mode(SaveMode.Overwrite).jdbc(connectionUrl,"employ_analysis_liunx",prop)
    //注册表 就业与待业人员分析表
    studentsDF.registerTempTable("employ_analysis_tmp")
    // 默认降序
    val df = sqlContext.sql("select * from employ_analysis_tmp order by employ")
    df.rdd.collect().foreach(row => println(row))
    sc.stop()
  }
}
