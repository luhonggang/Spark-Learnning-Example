package com.sanlen.person

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ author: LuHongGang
  * @ time: 2017/11/10
  * @ version: 1.0
  */
object PersonAnalysis {
  private val config = ConfigFactory.load()
  private val sc = new SparkContext(new SparkConf().setMaster(config.getString("spark.master")).setAppName("application.name"))
  private val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  private val hdfsPrefixPath = config.getString("hdfs.prefix.path")
  private val connectionUrl = config.getString("spark.mysql.connection.url")
  private val username = config.getString("spark.mysql.usernmae")
  private val password = config.getString("spark.mysql.password")

  case class Person(year: String, totalPerson: String, manNum: String, manRate: String, womenNum: String, womenRate: String, age: String,
                    deadAmount: String, beingMoney: String, receiveMoney: String, deadPaid: String, deadMoney: String)

  case class PeopleAnalysis(totalPerson: Double, manNum: Double, manRate: Double, womenNum: Double, womenRate: Double, age: Double, deadAmount: Double, beingMoney: Double,
                            receiveMoney: Double, deadPaid: Double, deadMoney: Double)

  def main(args: Array[String]): Unit = {
    //在Scala中使用反射方式，进行RDD到DataFrame的转换，需要手动导入一个隐式转换
    import sqlContext.implicits._
    val personDF = sc.textFile("C:\\Users\\Administrator\\Desktop\\human.txt").map(_.split(":")).map {
      p => Person(p(0).trim, p(1).trim, p(2).trim, p(3).trim, p(4).trim, p(5).trim, p(6).trim,
           p(7).trim, p(8).trim, p(9).trim, p(10).trim, p(11).trim)
    }.toDF()

    personDF.registerTempTable("person")
    val results = sqlContext.sql("select * from person order by year asc")
    results.show()
    loadAnalysisToMysql(results,"new")
    val avgValue = sqlContext.sql(
        "select avg(totalPerson) totalPerson,avg(manNum) manNum,avg(manRate) manRate," +
        "avg(womenNum) womenNum,avg(womenRate) womenRate," +
        "avg(age) age,avg(deadAmount) deadAmount,avg(beingMoney) beingMoney, " +
        "avg(receiveMoney) receiveMoney,avg(deadPaid) deadPaid,avg(deadMoney) deadMoney " +
        "FROM person").toDF()

    val totalAvg = avgValue.selectExpr(
      "totalPerson", "manNum", "round(manRate) as manRate", "womenNum",
       "round(womenRate) womenRate", "round(age) as age", "deadAmount",
      "beingMoney", "receiveMoney", "deadPaid", "deadMoney").toDF()
    val totalAvgRDD = totalAvg.rdd
    // 获取指定的几列的值
    // 还可以通过row的getValuesMap()方法，获取指定几列的值，返回的是个map
    val personRDD = totalAvgRDD.map { row => {
      val map = row.getValuesMap[Any](Array("totalPerson", "manNum", "manRate", "womenNum", "womenRate", "age", "deadAmount", "beingMoney", "receiveMoney", "deadPaid", "deadMoney"))
      PeopleAnalysis(
        map("totalPerson").toString().toDouble,
        map("manNum").toString().toDouble,
        map("manRate").toString().toDouble,
        map("womenNum").toString().toDouble,
        map("womenRate").toString().toDouble,
        map("age").toString().toDouble,
        map("deadAmount").toString().toDouble,
        map("beingMoney").toString().toDouble,
        map("receiveMoney").toString().toDouble,
        map("deadPaid").toString().toDouble,
        map("deadMoney").toString().toDouble)
    }
    }
    //personRDD.collect().foreach { stu => println(" 获取指定的字段的值是 ： " + stu.totalPerson + " : " + stu.manNum + " : " + stu.manRate) }

    val avgRDD = personRDD.collect()
    // 获取所有列的值
    val personRdd = personDF.rdd
    // 在scala中，可以用row的getAs()方法，获取指定列名的列
    personRdd.map { row => Person(
      row.getAs[String]("year"),
      row.getAs[String]("totalPerson"),
      row.getAs[String]("manNum"),
      row.getAs[String]("manRate"),
      row.getAs[String]("womenNum"),
      row.getAs[String]("womenRate"),
      row.getAs[String]("age"),
      row.getAs[String]("deadAmount"),
      row.getAs[String]("beingMoney"),
      row.getAs[String]("receiveMoney"),
      row.getAs[String]("deadPaid"),
      row.getAs[String]("deadMoney"))
    }
      .collect()
      .foreach { stu => println(" 获取所有字段的值 ： " + stu.manRate + " : " + stu.totalPerson + " : " + stu.manNum) }

    var yearDataRDD = sc.parallelize(Array(
      "5000,3000,1.00,3000,0.25,2.00,800,2000,3000,1500,1000",
      "5500,4100,1.25,3400,1.25,3.00,1000,3000,4000,1600,1200",
      "10000,4200,1.75,3500,1.00,4.00,1200,4000,4500,1800,1400",
      "11000,4300,2.00,3800,1.25,4.50,1500,5000,5000,1900,1600",
      "12000,4400,2.25,4000,1.50,5.00,1800,6000,5500,2000,2000")).map(_.split(","))

    // 指定字段 的类型
    val schemaType = StructType(
      List(
        StructField("totalPerson",DoubleType,true),
        StructField("manNum",DoubleType,true),
        StructField("manRate",DoubleType,true),
        StructField("womenNum",DoubleType,true),
        StructField("womenRate",DoubleType,true),
        StructField("age",DoubleType,true),
        StructField("deadAmount",DoubleType,true),
        StructField("beingMoney",DoubleType,true),
        StructField("receiveMoney",DoubleType,true),
        StructField("deadPaid",DoubleType,true),
        StructField("deadMoney",DoubleType,true)
      )
    )
    val peopleRDD = yearDataRDD.map(p => Row(
      p(0).trim.toDouble, p(1).trim.toDouble, p(2).trim.toDouble, p(3).trim.toDouble, p(4).trim.toDouble, p(5).trim.toDouble,
      p(6).trim.toDouble, p(7).trim.toDouble, p(8).trim.toDouble, p(9).trim.toDouble, p(10).trim.toDouble
    ))
    val featureDF =sqlContext.createDataFrame(peopleRDD,schemaType)
    val featureRDD =featureDF.rdd
    val featureData= featureRDD.map { row => PeopleAnalysis(
      row.getAs[Double]("totalPerson"),
      row.getAs[Double]("manNum"),
      row.getAs[Double]("manRate"),
      row.getAs[Double]("womenNum"),
      row.getAs[Double]("womenRate"),
      row.getAs[Double]("age"),
      row.getAs[Double]("deadAmount"),
      row.getAs[Double]("beingMoney"),
      row.getAs[Double]("receiveMoney"),
      row.getAs[Double]("deadPaid"),
      row.getAs[Double]("deadMoney"))}.collect()

    //预测2012年后的数据
    val arrayMetaData = Array("2013","2014","2015","2016","2017")
    for (i <- 0 until featureData.length) {
        var womanNum = subtract((featureData(i).totalPerson + avgRDD(0).totalPerson), (featureData(i).manNum + avgRDD(0).manNum))
        var manRate = divide((featureData(i).manNum+avgRDD(0).manNum),(featureData(i).totalPerson+avgRDD(0).totalPerson))
        var womenRate: String = divide((featureData(i).womenNum+avgRDD(0).womenNum),(featureData(i).totalPerson+avgRDD(0).totalPerson))
        arrayMetaData(i) =arrayMetaData(i)+","+(featureData(i).totalPerson+avgRDD(0).totalPerson)+"," +(featureData(i).manNum+avgRDD(0).manNum)+","+
          manRate+","+womanNum+","+womenRate+","+
        (featureData(i).age+avgRDD(0).age)+","+(avgRDD(0).deadAmount-featureData(i).deadAmount)+","+(featureData(i).beingMoney+avgRDD(0).beingMoney)+","+
        (featureData(i).receiveMoney+avgRDD(0).receiveMoney)+","+(featureData(i).deadPaid+avgRDD(0).deadPaid)+","+(featureData(i).deadMoney+avgRDD(0).deadMoney)
        println("数组的数据是 : "+arrayMetaData(i))
    }

    val dataRDD =sc.parallelize(arrayMetaData,5).map(_.split(",")).map {
      p => Person(p(0).trim, p(1).trim, p(2).trim, p(3).trim, p(4).trim, p(5).trim, p(6).trim,
        p(7).trim, p(8).trim, p(9).trim, p(10).trim, p(11).trim)
    }.toDF()
    // 数据映射存储到库中  打印数据详情
    println("------------------------------>>数据详情显示<<------------------------------")
      dataRDD.show()

    loadAnalysisToMysql(dataRDD,"new")
    println("------------------------------>>数据插入成功<<------------------------------")

  }

  def loadAnalysisToMysql(rdd: DataFrame,t: String):Unit={
    val prop = new java.util.Properties
    prop.setProperty("user", username)
    prop.setProperty("password", password)
    if(!"new".equals(t)){
      rdd.write.mode(SaveMode.Overwrite).jdbc(connectionUrl, "human_analysis", prop)
    }else{
      rdd.write.mode(SaveMode.Append).jdbc(connectionUrl, "human_analysis", prop)
    }
  }

  def add(a:Double, b:Double):Double=(a+b)
  def subtract(a:Double,b:Double):Double=(a-b)
  def divide(a:Double,b:Double):String=((a/b)*100).formatted("%.2f")
}
