package com.streaming.localhandle

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * author : LuHongGang
  * time   : 2017/11/20
  * version: 1.0
  */
object SQLContextSingleton {
  @transient  private var instance: SQLContext = _
  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}
