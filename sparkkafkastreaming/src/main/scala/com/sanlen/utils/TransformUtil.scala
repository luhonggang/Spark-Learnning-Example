package com.streaming.utils

/** 转换类
  * @ author: LuHongGang
  * @ time:   2017/11/14
  * @ version: 1.0
  */
object TransformUtil {
   /**
    * 将String转换成Int
    */
  private object IntParam {
    def applyToNumber(str: String): Option[Int] = {
      try {
        Some(str.toInt)
      } catch {
        case e: NumberFormatException => None
      }
    }
  }
}
