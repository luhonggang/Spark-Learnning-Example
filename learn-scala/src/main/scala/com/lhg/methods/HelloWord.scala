package com.lhg.methods

/**
  * scala 函数的定义与调用
  * @ author: LuHongGang
  * @ time: 2017/11/13
  * @ version: 1.0
  */
object HelloWord {
  def main(args: Array[String]): Unit = {
    myFunctions("hello")
    println("计算求和的结果 : "+sum(10))
    println("函数调用的结果 : "+fab(3))
  }

  /**
    * scala 要求声明函数的时候,必须给出函数的参数类型,但是不一定非要给出函数的返回值类型,
    * 只要右侧的函数体中部包含递归的语句.scala就会自动依据右侧的函数体表达式推断出函数的返回值类型
    */
  def myFunctions(name:String)={
   if("hello".equals(name)){
     println("Hello : This is My Function")
   }else{
     println("Hello : 不需要定义返回值类型")
   }
  }

  /**
    * 在代码块中定义包含多行语句的函数体,代码块中最后一行的代码的返回值就是整个函数的返回值,
    * 与Java不同的是,scala 不是使用return 返回值的
    */
  def sum(number:Int)={
    var sum = 0
    for(i <- 1 to number){
      sum+=i
    }
    sum

    /** 计算部分函数体 可替换为:
      * for(i <- 1 to number) sum+=i
      *  sum
      */
  }

  /**
    * 递归函数与返回值
    * 若在函数体内递归调用函数自身,则必须手动给出函数返回值类型
    * 如:实现经典的斐波那契数列
    */
  def fab(n: Int): Int = {
    if(n <= 1) 1
    else fab(n - 1) + fab(n - 2)
  }

}
