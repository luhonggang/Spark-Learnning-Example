package com.streaming.utils
import java.sql.{Connection, PreparedStatement, ResultSet}
import org.apache.commons.dbcp.BasicDataSource

/**
  * 数据库连接池代码
  * @ author: LuHongGang
  * @ time:   2017/11/14
  * @ version: 1.0
  */
object ConnectPoolUtil {
  private var bs:BasicDataSource = null

  /**
    * 创建数据源
    * @return
    */
  def getDataSource():BasicDataSource={
    if(bs==null){
      bs = new BasicDataSource()
      bs.setDriverClassName("com.mysql.jdbc.Driver")
      bs.setUrl("jdbc:mysql://10.0.0.24:3306/localdb")
      bs.setUsername("lhg")
      bs.setPassword("123456")
      bs.setMaxActive(200)           //设置最大并发数
      bs.setInitialSize(30)          //数据库初始化时，创建的连接个数
      bs.setMinIdle(50)              //最小空闲连接数
      bs.setMaxIdle(200)             //数据库最大连接数
      bs.setMaxWait(1000)
      bs.setMinEvictableIdleTimeMillis(60*1000)           //空闲连接60秒中后释放
      bs.setTimeBetweenEvictionRunsMillis(5*60*1000)      //5分钟检测一次是否有死掉的线程
      bs.setTestOnBorrow(true)
    }
    bs
  }

  /**
    * 释放数据源
    */
  def shutDownDataSource(){
    if(bs!=null){
      bs.close()
    }
  }

  /**
    * 获取数据库连接
    * @return
    */
  def getConnection():Connection={
    var con:Connection = null
    try {
      if(bs!=null){
        con = bs.getConnection()
      }else{
        con = getDataSource().getConnection()
      }
    } catch{
      case e:Exception => println(e.getMessage)
    }
    con
  }

  /**
    * 关闭连接
    */
  def closeConnection(rs:ResultSet ,ps:PreparedStatement,con:Connection){
    if(rs!=null){
      try {
        rs.close()
      } catch{
        case e:Exception => println(e.getMessage)
      }
    }
    if(ps!=null){
      try {
        ps.close()
      } catch{
        case e:Exception => println(e.getMessage)
      }
    }
    if(con!=null){
      try {
        con.close()
      } catch{
        case e:Exception => println(e.getMessage)
      }
    }
  }
}
