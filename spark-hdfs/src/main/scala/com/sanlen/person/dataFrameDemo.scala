package com.sanlen.person

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/** 注册成临时表
  * 源数据 类型 :  Michael, 29Andy, 30Justin, 19
  * @ author: LuHongGang
  * @ time: 2017/11/8
  * @ version: 1.0
  */
object dataFrameDemo {
  // 使用typesafe公司的config读取配置文件,默认读取application.properties
  private val config = ConfigFactory.load()
  private val sc = new SparkContext(new SparkConf().setMaster(config.getString("spark.master")).setAppName("application.name"))
  private val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  private val hdfsPrefixPath=config.getString("hdfs.prefix.path")
  private val connectionUrl=config.getString("spark.mysql.connection.url")
  private val username=config.getString("spark.mysql.usernmae")
  private val password=config.getString("spark.mysql.password")
  case class Person(name: String, age: Int)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    //val people = sc.textFile("C:\\Users\\Administrator\\Desktop\\person_data.txt")

    //"1995","121121","61808","51.03","59313","48.97","70.45","9000","35174","7510","3000","6000"
    val personRDD = sc.parallelize(Array(
      "1995,121121,61808,51.03,59313,48.97,70.45,9000,35174,7510,3000,6000",
      "1996,122389,62200,50.82,60189,49.18,75.15,8070,37304,7833,2070,6000",
      "1997,123626,63131,51.07,60495,48.93,76.95,8000,39449,8085,3000,5000",
      "1998,124761,63940,51.25,60821,48.75,77.49,7800,41608,8359,2800,4000",
      "1999,125786,64692,51.43,61094,48.57,79.10,7600,43748,8679,2800,4800",
      "2000,126743,65437,51.63,61306,48.37,78.81,7500,45906,8821,3000,4500",
      "2001,127627,65672,51.46,61955,48.54,80.32,7600,48064,9062,3500,4100",
      "2002,128453,66115,51.47,62338,48.53,80.05,7200,50212,9377,3700,3500",
      "2003,129227,66556,51.50,62671,48.50,82.69,6911,52376,9692,3911,3000",
      "2004,129988,66976,51.52,63012,48.48,79.72,6751,54283,9857,2751,4000",
      "2005,130756,67375,51.53,63381,48.47,80.00,6500,56212,10055,2400,4100",
      "2006,131448,67728,51.52,63720,48.48,79.01,6400,57706,10419,4200,2200",
      "2007,132129,68048,51.50,64081,48.50,79.27,5000,59379,10636,2500,2500",
      "2008,132802,68357,51.47,64445,48.53,82.25,4800,60667,10956,2000,2800",
      "2009,133450,68647,51.44,64803,48.56,85.81,4500,64512,11307,1500,3000",
      "2010,134091,68748,51.26,65343,48.74,89.70,3800,66978,11894,1200,2600",
      "2011,134735,69068,51.26,65667,48.74,88.19,3500,69079,12288,1000,2500",
      "2012,135404,69395,51.25,66009,48.75,90.12,3000,71182,12714,800,2200")).map(_.split(","))

    // 将源数据入库(Mysql)
   // personRDD.foreachPartition(myMySqlFun)

    // 数据对应的字段类型定义
    val schemaString = "year,totalPerson,totalMan,manRate,totalWoman,womanRate,personAge,dieNumber,beingPaid,oldMoney,diePaid,dieOldMoney"
    val schema = StructType(
      schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))

    // Convert records of the RDD (people) to Rows.
    val rowRDD = personRDD.map(x => Row(x(0).trim,x(1).trim,x(2).trim,x(3).trim,x(4).trim,x(5).trim,x(6).trim,x(7).trim,x(8).trim,x(9).trim,x(10).trim,x(11).trim))
    // 源数据与RDD的映射
    val peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema)


    // 注册临时表.
    peopleDataFrame.registerTempTable("people")
    // 测试查询 临时表的数据
    val prop = new Properties()
    prop.put("user", username)
    prop.put("password", password)
    val  results = sqlContext.sql("select * from people order by year asc")
    results.write.mode(SaveMode.Overwrite).jdbc(connectionUrl,"human_analysis",prop)

    results.map(t => "year: " + t(0)).collect().foreach(println)

   //计算出每一项的平均值
    val result =sqlContext.sql("select avg(totalPerson) as totalPerson,avg(totalMan) as totalMan,avg(manRate) as manRate," +
      "avg(totalWoman) as totalWoman,avg(womanRate) womanRate, avg(personAge) personAge,avg(dieNumber) dieNumber,avg(beingPaid) beingPaid," +
      "avg(oldMoney) oldMoney,avg(diePaid) diePaid,avg(dieOldMoney) dieOldMoney from people ").cache()
    result.show()
    result.persist(StorageLevel.MEMORY_AND_DISK)
    result.toDF().registerTempTable("avg_value")
    val maxPersonList = sqlContext.sql("select MAX(totalPerson) totalPerson from people").toDF()
   // maxPersonList.write.mode("overwrite").parquet("C:\\Users\\Administrator\\Desktop\\maxPerson")


    //totalPersonAvg.show()
    //totalPersonAvg.map(t=>t(0)).collect()
    //随着时间的增长  总人口与时间成正比
    //result.foreachPartition(myMySqlFun)
    //var array = new Array[String](5)
    var year = Array("2013","2014","2015","2016","2017")
    //year(0)=year(0).concat(","+produceMaxValue(130000))
    year(1)=year(1).concat(",")
    year.foreach { x => print(x+",")}


  }
  /**
    * 借助ListBuffer,最高效的方式
    */
  def increment_MostEffective(list: List[Int]) : List[Int] = {
    import scala.collection.mutable.ListBuffer
    var buffer = new ListBuffer[Int]

    for (element <- list) buffer += element + 1
    buffer.toList
  }

  /*def produceMaxValue(number:Int):Int={
    var produceNumber:Int = 0
   // val formatNum = number.toInt
      if(number >= 50 && number <= 90){
        produceNumber=(new Random).nextInt(10)+number
        return produceNumber
      }else if(number >130000){
        produceNumber=(new Random).nextInt(20000)+number
        return produceNumber
      }else if(number < 8000){
        print("ni")
        return 200
      }else{
        print("ni")
        return  1000
      }
  }*/
}

