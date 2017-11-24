package com.sanlen.utils

import java.sql.Connection

import com.jolbox.bonecp.{BoneCP, BoneCPConfig}
import org.slf4j.LoggerFactory

object ConnectionPools {

  val logger = LoggerFactory.getLogger(this.getClass)
  private val connectionPool = {
    try {
      Class.forName("com.mysql.jdbc.Driver")
      val config = new BoneCPConfig()
      config.setJdbcUrl("jdbc:mysql://10.0.0.24:3306/spark_project")
      config.setUsername("lhg")
      config.setPassword("123456")
      config.setMinConnectionsPerPartition(2)
      config.setMaxConnectionsPerPartition(5)
      config.setPartitionCount(3)
      config.setCloseConnectionWatch(true)
      config.setLogStatementsEnabled(true)
      Some(new BoneCP(config))
    } catch {
      case exception: Exception =>
        logger.warn("Error in creation of connection pool" + exception.printStackTrace())
        None
    }
  }

  def getConnection: Option[Connection] = {
    connectionPool match {
      case Some(connPool) => Some(connPool.getConnection)
      case None => None
    }
  }
  def closeConnection(connection:Connection): Unit = {
    if(!connection.isClosed) connection.close()
  }
}