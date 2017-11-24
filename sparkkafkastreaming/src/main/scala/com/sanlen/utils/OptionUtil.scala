package com.sanlen.utils

import org.joda.time.DateTime

object OptionUtil {

  def replaceStr(str1:String,str2:String) : String={
    str1.replaceAll(str2,"")
  }

  def timeFormat(time:String,format:String):String={
   val str="dddd"
  str
  }

  def main(args: Array[String]): Unit = {
    val str = "'''''abcdefghijklmnopqrstuvwxyz"
    println("====================="+replaceStr(str,"'"))
    val strTime = "17/Jan/2017:00:52:46 +0800";
    val time:DateTime = new DateTime("17/Jan/2017:00:52:46 +0800")
    println(time)


  }

}
