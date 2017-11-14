package com.lhg.methods

/** 过程
  * @ author: LuHongGang
  * @ time:   2017/11/13
  * @ version: 1.0
  */
object Procedure {

  def main(args: Array[String]): Unit = {

    sayHello("XiaoMing")

    /**
      * 在Scala中，提供了lazy值的特性，也就是说，如果将一个变量声明为lazy，
      * 则只有在第一次使用该变量时，变量对应的表达式才会发生计算。
      * 这种特性对于特别耗时的计算操作特别有用，比如打开文件进行IO，进行网络IO等
      */
    import scala.io.Source._
    lazy val lines = fromFile("C:\\Users\\Administrator\\Desktop\\human1.txt").mkString
   // 即使文件不存在，也不会报错，只有第一个使用变量时会报错，证明了表达式计算的lazy特性。
   // println("输出lazy值 : "+lines)
  }

  /**
    * 在scala中,若函数体直接包裹在了花括号中,而没有用=号连接 ,则函数的返回值类型就是Unit
    * 这样的函数就是过程,过程 通常用于不需要返回值的函数
    */
//  def sayHello(name:String):Unit={
//   println("Say Hello To "+name)
//  }
  def sayHello(name:String){println("Say Hello To "+name)}
  //def sayHello(name:String)="hello "+name
  //def sayHello(name: String): Unit = "Hello, " + name

}
