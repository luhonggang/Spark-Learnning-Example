package com.sanlen.spark

import java.sql.{DriverManager, PreparedStatement, Connection}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * 当前操作必须确保数据库中表 要先存在
  * @ author: LuHongGang
  * @ time: 2017/11/2
  * @ version: 1.0
  */
object LoadToMysql {
  // 定义对象
  //case class Blog(name: String, count: Int)
  case class Person(year:String,totalPerson:String,totalMan:String,manRate:String,totalWoman:String,womanRate:String,personAge:String,
                    dieNumber:String,beingPaid:String,oldMoney:String,diePaid:String,dieOldMoney:String)
  def myMySqlFun(iterator:Iterator[(String,String,String,String,String,String,String,String,String,String,String,String)]): Unit ={
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "insert into Person(year,totalPerson,totalMan,manRate,totalWoman,womanRate,personAge,dieNumber,beingPaid,oldMoney,diePaid,dieOldMoney) " +
      "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    try {
      conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/localdb", "lhg", "123456")
      iterator.foreach(data => {
        ps = conn.prepareStatement(sql)
        ps.setString(1, data._1)
        ps.setString(2, data._2)
        ps.setString(3, data._3)
        ps.setString(4, data._4)
        ps.setString(5, data._5)
        ps.setString(6, data._6)
        ps.setString(7, data._7)
        ps.setString(8, data._8)
        ps.setString(9, data._9)
        ps.setString(10, data._10)
        ps.setString(11, data._11)
        ps.setString(12, data._12)
        ps.executeUpdate()
      }
      )
    } catch {
      case e: Exception => println("Mysql Exception ------> 数据库表必须先创建 : "+e)
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("LoadToMysql").setMaster("local")
    val sc = new SparkContext(conf)
    //构造一个已存在的集合 来初始化 RDD  用SparkContext的paralleize方法
    val data = sc.parallelize(List(("1995","121121","61808","51.03","59313","48.97","70.45","9000","35174","7510","3000","6000"), ("1996","121121","61808","51.03","59313","48.97","70.45","9000","35174","7510","3000","6000")))
    data.foreachPartition(myMySqlFun)
  }

}
