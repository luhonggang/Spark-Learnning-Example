package com.lhg.methods

/** 默认参数
  * @ author: LuHongGang
  * @ time:   2017/11/13
  * @ version: 1.0
  */
object DefaultParmeter {
  def main(args: Array[String]): Unit = {
   println("结果 : "+ sayHello("Hello"))

    /**
      * 在调用函数时，也可以不按照函数定义的参数顺序来传递参数，而是使用带名参数的方式来传递。
      */
   println("结果 : "+ sayHello(firstName = "Mick", lastName = "Nina", middleName = "Jack"))
    /**
      * 还可以混合使用未命名参数和带名参数，但是未命名参数必须排在带名参数前面。
      */
   println("结果 : "+ sayHello("Mick", lastName = "Nina", middleName = "Jack"))

  }

  /**
    * 在Scala中，有时我们调用某些函数时，不希望给出参数的具体值，
    * 而希望使用参数自身默认的值，此时就定义在定义函数时使用默认参数。
    * 如果给出的参数不够，则会从左往右依次应用参数。
    */
  def sayHello(firstName: String, middleName: String = "William", lastName: String = "Croft") = firstName + " " + middleName + " " + lastName

}
