package com.sanlen

/**
  * @ author: LuHongGang
  * @ time: 2017/11/9
  * @ version: 1.0
  */
object testArray {
  def main(args: Array[String]): Unit = {

    var list = Array("2013","2014","2015","2016","2017")
    //list.updated(2,"hadoop1dddd")
    list(0)=list(0).concat(",135555555")
    list.foreach { x => print(x)}
  }

}
